from __future__ import annotations

import threading
from pathlib import Path
from queue import Queue
from time import sleep, time

from config.config_loader import get_config
from ingestion import file_tracker, file_watcher
from ingestion.file_reader import read_file
from utils.logger import get_logger
from utils.alerter import send_alert
from validation.validation_runner import validation_runner

# DWH imports
from datawarehouse.db_connection import init_pool, close_pool
from datawarehouse.schema_init   import init_schema
from datawarehouse.dim_loader    import load_dimension
from datawarehouse.fact_loader   import load_fact
from datawarehouse.file_log      import record as log_file

logger = get_logger(__name__)

_cfg      = get_config()
_stop_evt = threading.Event()

_batch_thread:  threading.Thread | None = None
_stream_thread: threading.Thread | None = None

# Built once from config — {filename: {"table": "Dim_X", ...}}
_ALL_FILES: dict = {
    **_cfg["watcher"]["batch"]["expected_files"],
    **_cfg["watcher"]["stream"]["expected_files"],
}

# Logical file name → DWH table name (from config)
_FILE_TO_TABLE: dict[str, str] = {
    # batch
    "customers":         "dim_customer",
    "drivers":           "dim_driver",
    "agents":            "dim_agent",
    "regions":           "dim_region",
    "reasons":           "dim_reason",
    "categories":        "dim_category",
    "segments":          "dim_segment",
    "teams":             "dim_team",
    "channels":          "dim_channel",
    "priorities":        "dim_priority",
    "reason_categories": "dim_reason_category",
    "restaurants":       "dim_restaurant",
    "cities":            "dim_region",       # denormalised
    # stream
    "orders":            "fact_order",
    "tickets":           "fact_ticket",
    "ticket_events":     "fact_ticket_event",
}

# Which file types are facts vs dimensions
_FACT_FILES = {"orders", "tickets", "ticket_events"}
_DIM_FILES  = set(_FILE_TO_TABLE.keys()) - _FACT_FILES


# ── File processing ──────────────────────────────────────────────────────────

def _process_file(path: Path) -> None:
    """
    Full ingestion pipeline for a single file.
    Never raises — every error is caught, logged, and skipped.
    """
    file_path = str(path)
    filename  = path.name
    t_start   = time()

    sleep(0.5)

    # step 1: hash
    try:
        file_hash = file_tracker.compute_hash(file_path)
    except Exception as exc:
        logger.error(f"[runner] failed to hash {filename}: {exc}")
        return

    # step 2: idempotency check
    try:
        if file_tracker.is_processed(file_path, file_hash):
            logger.info(f"[runner] already processed, skipping: {filename}")
            return
    except Exception as exc:
        logger.error(f"[runner] file_tracker check failed for {filename}: {exc}")
        return

    # step 3: read file
    try:
        df, file_type_name = read_file(path)
        logger.info(f"[runner] read ok — {filename} | type={file_type_name} | rows={len(df)}")
    except ValueError as exc:
        logger.error(f"[runner] read error (invalid file): {exc}")
        file_tracker.mark_as_failed(file_path, file_hash, str(exc))
        send_alert(error="Invalid File Error", message=f"Failed to read {filename}: {exc}")
        return
    except Exception as exc:
        logger.error(f"[runner] read error (parse failed): {filename} — {exc}")
        file_tracker.mark_as_failed(file_path, file_hash, str(exc))
        send_alert(error="Parse Failure", message=f"Failed to parse {filename}: {exc}")
        return

    # Resolve DWH table name and file kind
    source_table = _FILE_TO_TABLE.get(file_type_name, file_type_name)
    total_records = len(df)

    # step 4: validation
    try:
        validator = validation_runner(
            df,
            filename,
            file_path   = file_path,
            source_table= source_table,   # passed through to fault_handler
        )
        is_valid, clean_df, out_filename, processed_ts = validator.run()

        if not is_valid:
            error_msg = f"Schema validation failed completely for {filename}"
            logger.error(f"[runner] {error_msg}")
            file_tracker.mark_as_failed(file_path, file_hash, error_msg)
            send_alert(error="Validation Critical Failure", message=error_msg)
            log_file(
                file_path=file_path, file_name=filename, file_hash=file_hash,
                file_type=validator.file_type, source_table=source_table,
                status="failed", rejection_stage="schema",
                total_records=total_records, valid_records=0,
                quarantined=total_records,
            )
            return

        valid_count      = len(clean_df) if clean_df is not None else 0
        quarantined_count = total_records - valid_count

    except Exception as exc:
        error_msg = f"Unexpected error during validation of {filename}: {exc}"
        logger.error(f"[runner] {error_msg}")
        file_tracker.mark_as_failed(file_path, file_hash, str(exc))
        send_alert(error="Pipeline Exception", message=error_msg)
        return

    # step 4.5: load to DWH
    records_loaded = 0
    try:
        if clean_df is not None and not clean_df.empty:
            if file_type_name in _FACT_FILES:
                records_loaded = load_fact(file_type_name, clean_df)
            else:
                records_loaded = load_dimension(file_type_name, clean_df)
        else:
            logger.info(f"[runner] No valid records to load for {filename}")

    except Exception as exc:
        error_msg = f"DWH load failed for {filename}: {exc}"
        logger.error(f"[runner] {error_msg}")
        file_tracker.mark_as_failed(file_path, file_hash, error_msg)
        send_alert(error="DWH Load Failure", message=error_msg)
        log_file(
            file_path=file_path, file_name=filename, file_hash=file_hash,
            file_type=validator.file_type, source_table=source_table,
            status="failed", rejection_stage="load",
            total_records=total_records, valid_records=valid_count,
            quarantined=quarantined_count,
        )
        return

    # step 5: mark done in SQLite tracker + Postgres file log
    file_tracker.mark_as_done(file_path, file_hash, records_loaded)

    latency_ms = int((time() - t_start) * 1000)
    log_file(
        file_path       = file_path,
        file_name       = filename,
        file_hash       = file_hash,
        file_type       = validator.file_type,
        source_table    = source_table,
        status          = "success",
        rejection_stage = None,
        total_records   = total_records,
        valid_records   = records_loaded,
        quarantined     = quarantined_count,
    )

    logger.info(
        f"[runner] done — {filename} | "
        f"{records_loaded} records loaded | latency={latency_ms}ms"
    )


# ── Main loop ────────────────────────────────────────────────────────────────

def _run_loop(file_queue: Queue[Path]) -> None:
    while not _stop_evt.is_set():
        try:
            path = file_queue.get(timeout=2)
        except Exception:
            continue
        try:
            _process_file(path)
        except Exception as exc:
            logger.error(f"[runner] unexpected error: {exc}")
            send_alert(error="Critical Loop Error",
                       message=f"Main loop caught unhandled exception: {exc}")


# ── Public API ───────────────────────────────────────────────────────────────

def start() -> None:
    logger.info("[runner] starting ingestion layer")

    # Initialise DWH — create DB, pool, tables
    init_pool()
    init_schema()

    file_tracker.start()
    file_queue, batch_thread, stream_thread = file_watcher.start()

    global _batch_thread, _stream_thread
    _batch_thread  = batch_thread
    _stream_thread = stream_thread

    _run_loop(file_queue)


def stop() -> None:
    logger.info("[runner] stopping ingestion layer")
    _stop_evt.set()

    global _batch_thread, _stream_thread
    if _batch_thread is not None and _stream_thread is not None:
        file_watcher.stop(_batch_thread, _stream_thread)

    file_tracker.stop()
    close_pool()
