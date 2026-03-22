from __future__ import annotations

import hashlib
import sqlite3
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Literal

from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

_cfg    = get_config()
DB_PATH = Path(_cfg["tracker"]["db_path"])


# ─── Commands (what gets put into the tracker queue) ──────────────────────────

@dataclass
class _CheckCmd:
    """Ask: has this file+hash been successfully processed before?"""
    file_path : str
    file_hash : str
    future    : Future[bool]      # tracker thread puts the answer here 
     


@dataclass
class _MarkDoneCmd:
    """Tell tracker: file processed successfully."""
    file_path      : str
    file_hash      : str
    records_loaded : int


@dataclass
class _MarkFailedCmd:
    """Tell tracker: file failed — keep it retryable."""
    file_path  : str
    file_hash  : str
    error_msg  : str


# union type for the queue
_Command = _CheckCmd | _MarkDoneCmd | _MarkFailedCmd


# ─── Tracker thread ───────────────────────────────────────────────────────────

class _TrackerThread(threading.Thread):
    """
    The ONLY thread that touches SQLite.
    All reads and writes go through this thread via the command queue.
    Pipeline threads never block on disk — they just put a command and continue.
    """

    def __init__(self, cmd_queue: Queue[_Command]) -> None:
        super().__init__(daemon=True, name="TrackerThread")
        self.cmd_queue   = cmd_queue
        self._stop_event = threading.Event()

    def run(self) -> None:
        #logger.info("[tracker] thread started")

        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH))
        self._init_table(conn)

        while not self._stop_event.is_set():
            try:
                # block with timeout so stop_event is checked regularly
                cmd = self.cmd_queue.get(timeout=1)
            except Exception:
                continue

            try:
                if isinstance(cmd, _CheckCmd):
                    self._handle_check(conn, cmd)

                elif isinstance(cmd, _MarkDoneCmd):
                    self._handle_mark_done(conn, cmd)

                elif isinstance(cmd, _MarkFailedCmd):
                    self._handle_mark_failed(conn, cmd)

            except Exception as exc:
                # tracker errors must never crash the pipeline
                logger.error(f"[tracker] command failed: {exc}")
                if isinstance(cmd, _CheckCmd):
                    # resolve the future so caller is never left hanging
                    cmd.future.set_result(False)

        conn.close()
        #logger.info("[tracker] thread stopped")

    # ── SQLite ops ────────────────────────────────────────────────────────────

    def _init_table(self, conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_path      TEXT    NOT NULL,
                file_hash      TEXT    NOT NULL,
                status         TEXT    NOT NULL,   -- 'success' | 'failed'
                processed_at   TEXT    NOT NULL,
                records_loaded INTEGER DEFAULT 0,
                error_msg      TEXT    DEFAULT '',
                PRIMARY KEY (file_path, file_hash)
            )
        """)
        conn.commit()

    def _handle_check(
        self,
        conn: sqlite3.Connection,
        cmd:  _CheckCmd,
    ) -> None:
        row = conn.execute(
            """
            SELECT 1 FROM processed_files
            WHERE file_path = ? AND file_hash = ? AND status = 'success'
            """,
            (cmd.file_path, cmd.file_hash),
        ).fetchone()

        # put answer into the future — caller is waiting on this
        cmd.future.set_result(row is not None)

    def _handle_mark_done(
        self,
        conn: sqlite3.Connection,
        cmd:  _MarkDoneCmd,
    ) -> None:
        conn.execute(
            """
            INSERT INTO processed_files
                (file_path, file_hash, status, processed_at, records_loaded)
            VALUES (?, ?, 'success', datetime('now'), ?)
            ON CONFLICT(file_path, file_hash) DO UPDATE SET
                status         = 'success',
                processed_at   = datetime('now'),
                records_loaded = excluded.records_loaded,
                error_msg      = ''
            """,
            (cmd.file_path, cmd.file_hash, cmd.records_loaded),
        )
        conn.commit()

    def _handle_mark_failed(
        self,
        conn: sqlite3.Connection,
        cmd:  _MarkFailedCmd,
    ) -> None:
        conn.execute(
            """
            INSERT INTO processed_files
                (file_path, file_hash, status, processed_at, error_msg)
            VALUES (?, ?, 'failed', datetime('now'), ?)
            ON CONFLICT(file_path, file_hash) DO UPDATE SET
                status       = 'failed',
                processed_at = datetime('now'),
                error_msg    = excluded.error_msg
            """,
            (cmd.file_path, cmd.file_hash, cmd.error_msg),
        )
        conn.commit()

    def stop(self) -> None:
        self._stop_event.set()


# ─── Module-level state ───────────────────────────────────────────────────────

_cmd_queue:     Queue[_Command]   = Queue()
_tracker_thread: _TrackerThread | None = None


# ─── Public API ───────────────────────────────────────────────────────────────

def start() -> None:
    """Start the tracker thread. Called once from main.py at startup."""
    global _tracker_thread
    _tracker_thread = _TrackerThread(_cmd_queue)
    _tracker_thread.start()


def stop() -> None:
    """Stop the tracker thread. Called once from main.py at shutdown."""
    if _tracker_thread:
        _tracker_thread.stop()


def compute_hash(file_path: str) -> str:
    """
    MD5 hash of file contents.
    Fast, collision-safe for our use case (change detection)
    Called by ingestion_runner before is_processed() check
    """
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def is_processed(file_path: str, file_hash: str) -> bool:
    """
    Non-blocking ask: has this exact file+hash been successfully processed?

    Puts a _CheckCmd into the queue and gets a Future back.
    The Future.result() call will wait only if the tracker thread hasn't
    answered yet — in practice it's near-instant since SQLite reads are fast.

    Pipeline is never blocked on disk — only on the in-memory Future.
    """
    future: Future[bool] = Future()
    _cmd_queue.put(_CheckCmd(
        file_path = file_path,
        file_hash = file_hash,
        future    = future,
    ))
    return future.result()   # waits for tracker thread answer, but never for disk I/O , so should be very fast ,even on retries


def mark_as_done(
    file_path:      str,
    file_hash:      str,
    records_loaded: int,
) -> None:
    """
    Fire-and-forget: tell tracker this file succeeded.
    Returns immediately — tracker thread writes to SQLite in background.
    """
    _cmd_queue.put(_MarkDoneCmd(
        file_path      = file_path,
        file_hash      = file_hash,
        records_loaded = records_loaded,
    ))


def mark_as_failed(
    file_path: str,
    file_hash: str,
    error_msg: str,
) -> None:
    """
    Fire-and-forget: tell tracker this file failed.
    Returns immediately — tracker thread writes to SQLite in background.
    Failed files are retried on the next pipeline run.
    """
    _cmd_queue.put(_MarkFailedCmd(
        file_path = file_path,
        file_hash = file_hash,
        error_msg = error_msg,
    ))