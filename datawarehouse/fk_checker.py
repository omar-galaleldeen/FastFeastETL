"""
fk_checker.py
=============
Replaces the SQLite reference.db used by orphan_validator and
batch_records_validator.

Provides two things:
  1. get_valid_ids(table, pk_col) — fetch all existing PKs from a dim table
  2. check_fk(df, fk_col, ref_table, ref_pk) — return (clean_df, orphan_df)

All lookups go directly to the fastfeastapp Postgres database.
The SQLite reference.db is retired — no longer written or read.
"""
from __future__ import annotations

import pandas as pd

from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)


def get_valid_ids(table: str, pk_col: str) -> set:
    """
    Fetch all existing primary key values from a dimension or fact table.

    Parameters
    ----------
    table   : Postgres table name, e.g. "dim_customer"
    pk_col  : Primary key column name, e.g. "customer_id"

    Returns
    -------
    set of values (as strings for consistent comparison with source DataFrames)
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT "{pk_col}" FROM "{table}"')
        rows = cur.fetchall()
        cur.close()
        return {str(row[0]) for row in rows}
    except Exception as exc:
        logger.error(f"[fk_checker] Failed to fetch ids from {table}.{pk_col}: {exc}")
        return set()
    finally:
        put_conn(conn)


def check_fk(
    df:        pd.DataFrame,
    fk_col:    str,
    ref_table: str,
    ref_pk:    str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Check a FK column in df against existing ids in ref_table.

    Parameters
    ----------
    df        : DataFrame to check
    fk_col    : Column in df that holds the FK value
    ref_table : Referenced Postgres table name (e.g. "dim_customer")
    ref_pk    : Referenced PK column name    (e.g. "customer_id")

    Returns
    -------
    (clean_df, orphan_df)
      clean_df  — rows where fk_col exists in ref_table
      orphan_df — rows where fk_col has no match (potential orphans)
    """
    if fk_col not in df.columns:
        logger.warning(f"[fk_checker] Column '{fk_col}' not in DataFrame — skipping.")
        return df, pd.DataFrame()

    valid_ids   = get_valid_ids(ref_table, ref_pk)
    orphan_mask = df[fk_col].notna() & ~df[fk_col].astype(str).isin(valid_ids)
    orphan_df   = df[orphan_mask].copy()
    clean_df    = df[~orphan_mask].copy()

    if not orphan_df.empty:
        logger.warning(
            f"[fk_checker] {len(orphan_df)} orphan(s) on '{fk_col}' "
            f"→ {ref_table}.{ref_pk}"
        )

    return clean_df, orphan_df


# ── Table name mapping ────────────────────────────────────────────────────────
# Maps the logical reference name used in pipeline_config.yaml foreign_keys
# section to the actual Postgres table name and its PK column.
# Used by orphan_validator to resolve "customers" → ("dim_customer", "customer_id")

FK_TABLE_MAP: dict[str, tuple[str, str]] = {
    "customers":   ("dim_customer",   "customer_id"),
    "drivers":     ("dim_driver",     "driver_id"),
    "restaurants": ("dim_restaurant", "restaurant_id"),
    "agents":      ("dim_agent",      "agent_id"),
    "orders":      ("fact_order",     "order_id"),
    "tickets":     ("fact_ticket",    "ticket_id"),
}


def resolve_ref_table(logical_name: str) -> tuple[str, str] | None:
    """
    Convert a config FK reference name to (postgres_table, pk_col).

    Parameters
    ----------
    logical_name : value from pipeline_config.yaml foreign_keys,
                   e.g. "customers", "orders"

    Returns
    -------
    (table_name, pk_col) or None if not found
    """
    result = FK_TABLE_MAP.get(logical_name)
    if result is None:
        logger.error(f"[fk_checker] Unknown FK reference: '{logical_name}'")
    return result
