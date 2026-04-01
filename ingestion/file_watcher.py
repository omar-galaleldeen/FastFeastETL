from __future__ import annotations

import threading
import time
from datetime import date
from pathlib import Path
from queue import Queue

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Load config once at module level ────────────────────────────────────────

_cfg         = get_config()
BATCH_FILES:  list[str] = list(_cfg["watcher"]["batch"]["expected_files"].keys())
STREAM_FILES: list[str] = list(_cfg["watcher"]["stream"]["expected_files"].keys())


# ─── Stream handler (watchdog) ───────────────────────────────────────────────

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

# Force the path to be a string to satisfy type checker
        path = Path(str(event.src_path))
        if path.name not in STREAM_FILES:
            return                          # silently ignore unexpected files

        self.file_queue.put(path)           # non-blocking, thread-safe


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

        # _stop_event.wait() blocks here until stop() sets the event.
        # much cleaner than while + time.sleep(1) loop —
        # thread wakes up immediately when stop() is called,
        # not after waiting out the sleep interval.
        self._stop_event.wait()

        observer.stop()
        observer.join()

        logger.info("[stream] observer stopped")

    def stop(self) -> None:
        self._stop_event.set()          # unblocks wait() immediately


# ─── Batch thread ─────────────────────────────────────────────────────────────

class BatchWatcherThread(threading.Thread):
    """
    Daemon thread — scans once per day at trigger_hour.
    Uses _stop_event.wait(timeout=30) instead of time.sleep(30) —
    thread responds to stop() immediately rather than waiting out the sleep.
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
        self._scanned_today: str = ""   # tracks which date was already scanned

    def run(self) -> None:
        logger.info("[batch] watcher started")

        while not self._stop_event.is_set():
            now      = time.localtime()
            today    = str(date.today())
            at_hour  = now.tm_hour == self.trigger_hour

            # scan only if:
            #   - we are at the trigger hour
            #   - we haven't already scanned today
            if at_hour and self._scanned_today != today:
                self._scan()
                self._scanned_today = today     # mark today as scanned

            # wait(timeout=30):
            #   - if stop() is called → event is set → wait() returns True immediately
            #   - if not stopped      → wait() returns False after 30s
            # either way we loop back and check the clock again
            self._stop_event.wait(timeout=30)

        logger.info("[batch] watcher stopped")

    def _scan(self) -> None:
        today_dir = self.batch_dir / str(date.today())

        if not today_dir.exists():
            logger.warning(f"[batch] today_dir not found: {today_dir}")
            return

        found = (
            sorted(today_dir.glob("*.csv")) +
            sorted(today_dir.glob("*.json"))
        )

        queued = 0
        for f in found:
            if f.name in BATCH_FILES :
                self.file_queue.put(f)
                queued += 1

        logger.info(f"[batch] scan done — {queued} file(s) queued from {today_dir}")

    def stop(self) -> None:
        self._stop_event.set()          # unblocks wait(timeout=30) immediately


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

    # ADDED: Ensure directories exist before watching them
    batch_dir.mkdir(parents=True, exist_ok=True)
    stream_dir.mkdir(parents=True, exist_ok=True)

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