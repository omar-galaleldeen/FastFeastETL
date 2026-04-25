"""
master_seeder.py
================
Ensures all source data in data/master/ is ingested into the DWH
before the pipeline starts watching for new files.

How it works:
  1. Scans data/master/ for all known source files
  2. For each file, computes its MD5 hash
  3. Checks the SQLite file_tracker — if already ingested with this
     exact hash, skips it (idempotent)
  4. If not ingested, runs the file through the full pipeline:
     read → validate → load → mark done
  5. Only after ALL master files are confirmed ingested does
     ingestion_runner proceed to start the watchers

This runs synchronously on every startup — it is fast because
the idempotency check short-circuits on subsequent runs.

Master file → logical type mapping:
  Master uses .csv for restaurants and cities (OLTP format).
  Batch uses .json for the same files. The seeder handles the
  mapping so validation and loading work correctly.
"""
from __future__ import annotations

from pathlib import Path
from time import time

from config.config_loader import get_config
from ingestion import file_tracker
from ingestion.file_reader import _read_csv, _read_json
from utils.logger import get_logger
from utils.alerter import send_alert

logger = get_logger(__name__)

_cfg = get_config()

# ── Master directory ─────────────────────────────────────────────────────────
MASTER_DIR = Path(_cfg.get("master", {}).get("dir", "data/master"))

# ── Master file list ─────────────────────────────────────────────────────────
# Maps master filename → logical type name used by schema_registry,
# validation_runner, and dim_loader.
# Order matters: parent dims before children (region before customer, etc.)
MASTER_FILES: list[tuple[str, str]] = [
    ("cities.csv",            "cities"),           # .csv in master, .json in batch — MUST come before regions
    ("regions.csv",           "regions"),
    ("segments.csv",          "segments"),
    ("categories.csv",        "categories"),
    ("teams.csv",             "teams"),
    ("reason_categories.csv", "reason_categories"),
    ("reasons.csv",           "reasons"),
    ("channels.csv",          "channels"),
    ("priorities.csv",        "priorities"),
    ("customers.csv",         "customers"),
    ("restaurants.csv",       "restaurants"),      # .csv in master, .json in batch
    ("drivers.csv",           "drivers"),
    ("agents.csv",            "agents"),
]


# ── Seeder ────────────────────────────────────────────────────────────────────

def seed_master_data() -> None:
    """
    Check and ingest all master source files before the pipeline starts.
    Called once from ingestion_runner.start() after init_schema().

    Logs a summary at the end:
      [seeder] Master seed complete — X loaded, Y skipped, Z failed
    """
    if not MASTER_DIR.exists():
        logger.warning(
            f"[seeder] Master directory not found: {MASTER_DIR}. "
            "Skipping master seed — pipeline will start without base data."
        )
        return

    loaded  = 0
    skipped = 0
    failed  = 0

    print(f"\n{'═' * 50}")
    print(f"🌱 Seeding master data from {MASTER_DIR}/")
    print(f"{'═' * 50}")

    for filename, file_type_name in MASTER_FILES:
        path = MASTER_DIR / filename

        if not path.exists():
            logger.warning(f"[seeder] Master file not found, skipping: {path}")
            skipped += 1
            continue

        result = _seed_file(path, file_type_name)

        if result == "skipped":
            skipped += 1
        elif result == "loaded":
            loaded += 1
        else:
            failed += 1

    print(f"{'═' * 50}")
    print(
        f"🌱 Master seed complete — "
        f"{loaded} loaded | {skipped} already ingested | {failed} failed"
    )
    print(f"{'═' * 50}\n")

    logger.info(
        "[seeder] Master seed complete",
        extra={"loaded": loaded, "skipped": skipped, "failed": failed},
    )

    if failed > 0:
        send_alert(
            error="Master Seed Failures",
            message=f"{failed} master file(s) failed to ingest. "
                    "Check logs for details. Pipeline will continue but "
                    "FK checks may fail for missing dimension data.",
        )


def _seed_file(path: Path, file_type_name: str) -> str:
    """
    Process a single master file through the full pipeline.

    Returns
    -------
    'skipped' — file already ingested with this hash
    'loaded'  — file successfully ingested
    'failed'  — file failed at any stage
    """
    # Lazy imports to avoid circular imports at module level
    from validation.validation_runner import validation_runner
    from datawarehouse.dim_loader import load_dimension
    from datawarehouse.file_log import record as log_file

    file_path = str(path)
    filename  = path.name
    t_start   = time()

    # ── Step 1: Hash ────────────────────────────────────────────────────── #
    try:
        file_hash = file_tracker.compute_hash(file_path)
    except Exception as exc:
        logger.error(f"[seeder] Failed to hash {filename}: {exc}")
        return "failed"

    # ── Step 2: Idempotency check ────────────────────────────────────────── #
    if file_tracker.is_processed(file_path, file_hash):
        logger.info(f"[seeder] Already ingested, skipping: {filename}")
        print(f"  ✓ {filename:<30} already ingested")
        return "skipped"

    print(f"  ↻ {filename:<30} ingesting...", end="", flush=True)

    # ── Step 3: Read ────────────────────────────────────────────────────── #
    try:
        # Master restaurants and cities are .csv — read as CSV regardless
        # validation and loading use file_type_name, not file extension
        df = _read_csv(path)
        logger.info(f"[seeder] Read {filename} — {len(df)} rows")
    except Exception as exc:
        logger.error(f"[seeder] Read failed for {filename}: {exc}")
        file_tracker.mark_as_failed(file_path, file_hash, str(exc))
        print(f" ✗ read error: {exc}")
        return "failed"

    total_records = len(df)

    # ── Step 4: Validate ─────────────────────────────────────────────────── #
    # Use the logical file_type_name so schema_registry finds the right schema.
    source_table = _get_source_table(file_type_name)

    try:
        # Construct a fake filename with the right extension so file_type
        # detection in validation_runner works correctly.
        # validation_runner strips the extension and lowercases — so passing
        # file_type_name + ".csv" gives it exactly what it needs.
        logical_filename = f"{file_type_name}.csv"

        validator = validation_runner(
            df,
            logical_filename,
            file_path    = file_path,
            source_table = source_table,
        )
        is_valid, clean_df, _, processed_ts = validator.run()

        if not is_valid:
            error_msg = f"Schema validation failed for master file {filename}"
            logger.error(f"[seeder] {error_msg}")
            file_tracker.mark_as_failed(file_path, file_hash, error_msg)
            log_file(
                file_path=file_path, file_name=filename, file_hash=file_hash,
                file_type="batch", source_table=source_table,
                status="failed", rejection_stage="schema",
                total_records=total_records, valid_records=0,
                quarantined=total_records,
            )
            print(f" ✗ schema validation failed")
            return "failed"

        valid_count       = len(clean_df) if clean_df is not None else 0
        quarantined_count = total_records - valid_count

    except Exception as exc:
        logger.error(f"[seeder] Validation error for {filename}: {exc}", exc_info=True)
        file_tracker.mark_as_failed(file_path, file_hash, str(exc))
        print(f" ✗ validation error: {exc}")
        return "failed"

    # ── Step 5: Load ─────────────────────────────────────────────────────── #
    try:
        records_loaded = load_dimension(file_type_name, clean_df)
    except Exception as exc:
        logger.error(f"[seeder] DWH load failed for {filename}: {exc}", exc_info=True)
        file_tracker.mark_as_failed(file_path, file_hash, str(exc))
        log_file(
            file_path=file_path, file_name=filename, file_hash=file_hash,
            file_type="batch", source_table=source_table,
            status="failed", rejection_stage="load",
            total_records=total_records, valid_records=valid_count,
            quarantined=quarantined_count,
        )
        print(f" ✗ load error: {exc}")
        return "failed"

    # ── Step 6: Mark done ────────────────────────────────────────────────── #
    file_tracker.mark_as_done(file_path, file_hash, records_loaded)

    latency_ms = int((time() - t_start) * 1000)
    log_file(
        file_path       = file_path,
        file_name       = filename,
        file_hash       = file_hash,
        file_type       = "batch",
        source_table    = source_table,
        status          = "success",
        rejection_stage = None,
        total_records   = total_records,
        valid_records   = records_loaded,
        quarantined     = quarantined_count,
    )

    logger.info(
        f"[seeder] Loaded {filename} → {source_table} | "
        f"{records_loaded} rows | {latency_ms}ms"
    )
    print(f" ✓ {records_loaded} rows → {source_table} ({latency_ms}ms)")
    return "loaded"


# ── Helpers ───────────────────────────────────────────────────────────────────

# Inline copy of _FILE_TO_TABLE to avoid circular import from ingestion_runner
_SOURCE_TABLE_MAP: dict[str, str] = {
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
    "cities":            "dim_region",
}


def _get_source_table(file_type_name: str) -> str:
    return _SOURCE_TABLE_MAP.get(file_type_name, file_type_name)
