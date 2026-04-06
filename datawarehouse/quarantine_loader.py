"""
quarantine_loader.py
====================
Writes rejected records into the Postgres quarantine table.
Called by fault_handler alongside its existing CSV write —
both destinations receive the same bad records.

The quarantine table uses JSONB for raw_data so one table handles
rejected rows from any source (orders, tickets, customers, etc.)
without needing a per-table schema.

Your stored procedure / trigger for promoting true orphans after 24h
should query:
    SELECT * FROM quarantine
    WHERE rejection_stage = 'orphan'
      AND quarantined_at < NOW() - INTERVAL '24 hours'
"""
from __future__ import annotations

import json
import pandas as pd
import psycopg2.extras

from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)


def write_quarantine(
    df:              pd.DataFrame,
    source_file:     str,
    source_table:    str,
    rejection_reason:str,
    rejection_stage: str,   # 'schema' | 'records' | 'orphan'
) -> None:
    """
    Insert rejected rows into the quarantine table.

    Parameters
    ----------
    df               : DataFrame of rejected rows
    source_file      : original filename, e.g. "orders.json"
    source_table     : target DWH table, e.g. "fact_order"
    rejection_reason : human-readable reason string
    rejection_stage  : one of 'schema', 'records', 'orphan'
    """
    if df is None or df.empty:
        return

    # Serialize each row to a JSON-compatible dict
    # NaT / NaN → None so JSON serialization doesn't break
    records = df.where(pd.notna(df), None).to_dict(orient="records")

    rows = [
        (
            source_file,
            source_table,
            rejection_reason,
            rejection_stage,
            json.dumps(record, default=str),   # JSONB column
        )
        for record in records
    ]

    sql = """
        INSERT INTO quarantine
            (source_file, source_table, rejection_reason, rejection_stage, raw_data)
        VALUES (%s, %s, %s, %s, %s::jsonb)
    """

    conn = get_conn()
    try:
        with conn:
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
            cur.close()
        logger.info(
            f"[quarantine] {len(rows)} records quarantined "
            f"from {source_file} → stage={rejection_stage}"
        )
    except Exception as exc:
        logger.error(f"[quarantine] Failed to write quarantine records: {exc}")
    finally:
        put_conn(conn)
