from __future__ import annotations

import threading
import time
from datetime import date
from pathlib import Path
from queue import Queue

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from ingestion.tracker import is_processed
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Load config once at module level ────────────────────────────────────────

_cfg         = get_config()
BATCH_FILES:  list[str] = _cfg["watcher"]["batch"]["expected_files"]
STREAM_FILES: list[str] = _cfg["watcher"]["stream"]["expected_files"]


# ─── Stream handler (watchdog) ────────────────────────────────────────────────

class StreamEventHandler(FileSystemEventHandler):
    """
    Fired instantly by watchdog when a file is created anywhere under
    data/input/stream/YYYY-MM-DD/HH/
    Does the minimum: validate name → check tracker → put in queue.
    No logging — must never block the OS event handler.
    """

    def __init__(self, file_queue: Queue[Path]) -> None:
        self.file_queue = file_queue

    def on_created(self, event) -> None:
        if event.is_directory:
            return

        path = Path(event.src_path)

        if path.name not in STREAM_FILES:
            return                      # silently ignore unexpected files

        if is_processed(str(path)):
            return                      # silently skip already-processed

        self.file_queue.put(path)       # non-blocking, thread-safe


# ─── Stream thread ────────────────────────────────────────────────────────────

class StreamWatcherThread(threading.Thread):
    """
    Daemon thread — runs a watchdog Observer on data/input/stream/.
    recursive=True so it catches files inside YYYY-MM-DD/HH/ subfolders.
    Only logs on start and stop.
    """

    def __init__(
        self,
        stream_dir: Path,
        file_queue: Queue[Path],
    ) -> None:
        super().__init__(daemon=True, name="StreamWatcher")
        self.stream_dir  = stream_dir
        self.file_queue  = file_queue
        self._stop_event = threading.Event()

    def run(self) -> None:
        handler  = StreamEventHandler(self.file_queue)
        observer = Observer()
        observer.schedule(handler, str(self.stream_dir), recursive=True)
        observer.start()

        logger.info("[stream] observer started")

        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()

        logger.info("[stream] observer stopped")

    def stop(self) -> None:
        self._stop_event.set()


# ─── Batch thread ─────────────────────────────────────────────────────────────

class BatchWatcherThread(threading.Thread):
    """
    Daemon thread — polls once per day at trigger_hour.
    Resolves today's dated subfolder: data/input/batch/YYYY-MM-DD/
    Uses flat glob — dated folder has no subfolders.
    Only logs on start and stop.
    """

    def __init__(
        self,
        batch_dir:    Path,
        file_queue:   Queue[Path],
        trigger_hour: int,
    ) -> None:
        super().__init__(daemon=True, name="BatchWatcher")
        self.batch_dir    = batch_dir
        self.file_queue   = file_queue
        self.trigger_hour = trigger_hour
        self._stop_event  = threading.Event()

    def run(self) -> None:
        logger.info("[batch] watcher started")

        while not self._stop_event.is_set():
            now = time.localtime()
            if now.tm_hour == self.trigger_hour and now.tm_min == 0:
                self._scan()
                time.sleep(60)          # avoid double-scan within same minute
            time.sleep(30)              # check clock every 30s

        logger.info("[batch] watcher stopped")

    def _scan(self) -> None:
        # resolve today's dated subfolder e.g. data/input/batch/2026-02-22/
        today_dir = self.batch_dir / str(date.today())

        if not today_dir.exists():
            return                      # folder not dropped yet — do nothing

        # flat glob — no subfolders inside dated batch folder
        found = (
            sorted(today_dir.glob("*.csv")) +
            sorted(today_dir.glob("*.json"))
        )

        # queue only files that match expected names and aren't processed yet
        for f in found:
            if f.name in BATCH_FILES and not is_processed(str(f)):
                self.file_queue.put(f)

    def stop(self) -> None:
        self._stop_event.set()


# ─── Public API ───────────────────────────────────────────────────────────────

def start() -> tuple[Queue[Path], BatchWatcherThread, StreamWatcherThread]:
    """
    Reads dirs and trigger_hour from config.
    Starts both watcher threads.
    Returns the shared queue and thread handles to ingestion_runner.
    """
    batch_dir    = Path(_cfg["watcher"]["batch"]["dir"])
    stream_dir   = Path(_cfg["watcher"]["stream"]["dir"])
    trigger_hour = int(_cfg["watcher"]["batch"]["trigger_hour"])

    file_queue: Queue[Path] = Queue()

    batch_thread  = BatchWatcherThread(batch_dir,  file_queue, trigger_hour)
    stream_thread = StreamWatcherThread(stream_dir, file_queue)

    batch_thread.start()
    stream_thread.start()

    return file_queue, batch_thread, stream_thread


def stop(
    batch_thread:  BatchWatcherThread,
    stream_thread: StreamWatcherThread,
) -> None:
    """Gracefully shuts down both watcher threads."""
    batch_thread.stop()
    stream_thread.stop()