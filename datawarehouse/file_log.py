"""
file_log.py
===========
Analytics-facing record of every file processed by the pipeline.
Writes one row per file to pipeline_file_log in Postgres.

This complements (not replaces) the SQLite file_tracker:
  - SQLite file_tracker  → idempotency / hash-based dedup (fast, internal)
  - pipeline_file_log    → analytics visibility, queryable by dashboards/team

Called from ingestion_runner after validation completes.
"""
from __future__ import annotations

from datetime import datetime

from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)


def record(
    file_path:       str,
    file_name:       str,
    file_hash:       str,
    file_type:       str,           # 'batch' | 'stream'
    source_table:    str | None,    # e.g. 'dim_customer'
    status:          str,           # 'success' | 'failed'
    rejection_stage: str | None,    # 'schema' | 'records' | 'orphan' | None
    total_records:   int = 0,
    valid_records:   int = 0,
    quarantined:     int = 0,
    duplicate_count: int = 0,
    orphan_count:    int = 0,
) -> None:
    """
    Insert one row into pipeline_file_log.

    Parameters
    ----------
    file_path       : full OS path to the processed file
    file_name       : filename only, e.g. "orders.json"
    file_hash       : MD5 hash computed by file_tracker
    file_type       : 'batch' or 'stream'
    source_table    : DWH target table name
    status          : 'success' or 'failed'
    rejection_stage : stage where failure occurred, or None on success
    total_records   : rows ingested from file
    valid_records   : rows successfully loaded to DWH
    quarantined     : rows sent to quarantine
    duplicate_count : duplicates removed during validation
    orphan_count    : orphan records detected
    """
    sql = """
        INSERT INTO pipeline_file_log (
            file_path, file_name, file_hash, file_type, source_table,
            status, rejection_stage,
            total_records, valid_records, quarantined,
            duplicate_count, orphan_count, processed_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s
        )
    """

    conn = get_conn()
    try:
        with conn:
            cur = conn.cursor()
            cur.execute(sql, (
                file_path, file_name, file_hash, file_type, source_table,
                status, rejection_stage,
                total_records, valid_records, quarantined,
                duplicate_count, orphan_count,
                datetime.now(),
            ))
            cur.close()
        logger.info(
            f"[file_log] Recorded {file_name} → status={status} | "
            f"valid={valid_records} | quarantined={quarantined}"
        )
    except Exception as exc:
        # File log failure must never stop the pipeline
        logger.error(f"[file_log] Failed to write pipeline_file_log: {exc}")
    finally:
        put_conn(conn)
