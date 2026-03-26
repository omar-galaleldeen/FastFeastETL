from __future__ import annotations

import threading
from pathlib import Path
from queue import Queue
from time import sleep

from config.config_loader import get_config
from ingestion import  file_tracker, file_watcher
from ingestion.file_reader import read_file
from utils.logger import get_logger

logger = get_logger(__name__)

_cfg      = get_config()
_stop_evt = threading.Event()

# built once from config — {filename: {"table": "Dim_X"}}
_ALL_FILES: dict = {
    **_cfg["watcher"]["batch"]["expected_files"],
    **_cfg["watcher"]["stream"]["expected_files"],
}


# File processing  
def _process_file(path: Path) -> None:
    """
    Full ingestion pipeline for a single file.
    Never raises — every error is caught, logged, and skipped.
   
    """
    file_path = str(path)
    filename  = path.name



    sleep(0.5)   # simulate some processing time
    # step 1: hash  
    try:
        file_hash =  file_tracker.compute_hash(file_path)
    except Exception as e:
        logger.error(f"[runner] failed to hash {filename}: {e}")
        return

    # step 2: idempotency check  
    try:
        if file_tracker.is_processed(file_path, file_hash):
            logger.info(f"[runner] already processed, skipping: {filename}")
            return
    except Exception as e:
        logger.error(f"[runner] file_tracker check failed for {filename}: {e}")
        return
 

    # step 4: read file 
    try:
        df, file_type = read_file(path)
        logger.info(
            f"[runner] read ok — {filename} | "
            f"type={file_type} | "
            f"rows={len(df)}"
        )
    except ValueError as e:
        # unknown file name or unsupported format
        logger.error(f"[runner] read error (invalid file): {e}")
        file_tracker.mark_as_failed(file_path, file_hash, str(e))
        return

    except Exception as e:
        # IO error, encoding error, malformed CSV/JSON
        logger.error(f"[runner] read error (parse failed): {filename} — {e}")
        file_tracker.mark_as_failed(file_path, file_hash, str(e))
       # _send_alert("Parse failure", filename, str(e))
        return
    # TODO: plug in validation layer here before loading
    # ── step 5: hand off to validation layer  
    # validation_runner receives df, file_type 
    # and returns (clean_df, records_loaded) or raises
    # try:
    #     from validation.validator_orchestrator import run as validate
    #     clean_df, records_loaded = validate(df, file_type )

    # except Exception as e:
    #     logger.error(f"[runner] validation failed: {filename} — {e}")
    #     file_tracker.mark_as_failed(file_path, file_hash, str(e))
    #    # _send_alert("Validation failure", filename, str(e))
    #     return


    # step 4: DRY RUN
    logger.info(
        f"[runner] DRY RUN — "
        f"file={filename} | "
        f"type={file_type} | "
        f"rows={len(df)} | "
        f"columns={list(df.columns)}"
    )
    records_loaded = len(df)

    # ── step 6: mark done  
    file_tracker.mark_as_done(file_path, file_hash, records_loaded)
    logger.info(
        f"[runner] done — {filename} | "
        f"{records_loaded} records passed validation"
    )





# Alert helper  

# def _send_alert(subject: str, filename: str, error: str) -> None:
#     """Sends async alert — never blocks the pipeline."""
#     try:
#         from utils.alerter import send_async
#         send_async(
#             subject = f"[FastFeast] {subject}: {filename}",
#             body    = error,
#         )
#     except Exception:
#         pass    # alerter failure must never affect the pipeline


# Main loop 

def _run_loop(file_queue: Queue[Path]) -> None:
    """
    Drains the file queue forever.
    Blocked on queue.get() when idle but otherwise non-blocking — the processing is done in-memory and
    Stops cleanly when _stop_evt is set and queue is empty.
    """
     

    while not _stop_evt.is_set():
        try:
            # timeout so we can check _stop_evt regularly
            path = file_queue.get(timeout=2)
        except Exception:
            continue                    # timeout — loop back and check stop_evt

        try:
            _process_file(path)
        except Exception as e:
            # last-resort catch — _process_file should never raise
            # but if it does the loop must survive
            logger.error(f"[runner] unexpected error: {e}")

 
# Public API  
def start() -> None:
    """
    Called once by main.py.
    Starts file_tracker + watcher then runs the file processing loop.
    Blocks until stop() is called.
    """
    logger.info("[runner] starting ingestion layer")

    file_tracker.start()

    file_queue, batch_thread, stream_thread = file_watcher.start()

    # store thread handles so stop() can shut them down
    global _batch_thread, _stream_thread
    _batch_thread  = batch_thread
    _stream_thread = stream_thread

    # blocks here — runs until _stop_evt is set
    _run_loop(file_queue)


def stop() -> None:
    """
    Called once by main.py at shutdown.
    Signals the loop to stop, then shuts down watcher + file_tracker.
    """
    logger.info("[runner] stopping ingestion layer")

    _stop_evt.set()

    file_watcher.stop(_batch_thread, _stream_thread)
    file_tracker.stop()


# ─── thread handles (set by start())  

_batch_thread  = None
_stream_thread = None