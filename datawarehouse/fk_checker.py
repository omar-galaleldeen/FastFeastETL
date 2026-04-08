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

Fix notes
---------
* Float ID normalization: schema_validator coerces int columns to numeric, so
  a source value of 370 may arrive as the float 370.0, which str() turns into
  "370.0". Postgres stores the PK as the integer 370, which str() turns into
  "370". These two strings never match, producing false orphan rejections.
  _normalize_id() strips the trailing ".0" so both sides compare as "370".

* Config-driven FK_TABLE_MAP: the hardcoded dict has been replaced by
  _build_fk_table_map(), which derives the same information from
  pipeline_config.yaml at import time. Add or rename a dimension table in the
  config and this module picks it up automatically — no code change needed.
"""
from __future__ import annotations

import pandas as pd

from config.config_loader import get_config
from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)


# ── ID normalisation ─────────────────────────────────────────────────────────

def _normalize_id(value) -> str:
    """
    Normalise a FK value to a plain integer string for comparison.

    Handles the common case where schema_validator's pd.to_numeric coercion
    turns the integer 370 into the float 370.0, which str() then renders as
    "370.0" — causing a false mismatch against the Postgres PK "370".

    Non-numeric values (e.g. UUID strings for order_id) are returned as-is
    after stripping surrounding whitespace.

    Examples
    --------
    _normalize_id(370)      → "370"
    _normalize_id(370.0)    → "370"
    _normalize_id("370.0")  → "370"
    _normalize_id("370")    → "370"
    _normalize_id("f2e357") → "f2e357"   ← UUID, left unchanged
    """
    s = str(value).strip()
    if s.endswith(".0"):
        try:
            return str(int(float(s)))
        except (ValueError, OverflowError):
            pass
    return s


# ── Config-driven FK table map ────────────────────────────────────────────────

# Maps logical dimension/fact reference name → (postgres_table, pk_col).
# Built once at import time from pipeline_config.yaml so there is no
# hardcoding here — add a new dimension in the config and it just works.
#
# Sources used from the config:
#   reader.file_type_map          → filename → logical_name (e.g. "customers")
#   watcher.batch/stream          → logical_name → DWH table (e.g. "Dim_Customer")
#   schemas.<logical_name>        → primary_key column name
#
# The DWH table name in the config uses PascalCase ("Dim_Customer") while
# Postgres uses snake_case ("dim_customer"). We lower-case it on the way in.

def _build_fk_table_map() -> dict[str, tuple[str, str]]:
    """
    Build {logical_name: (postgres_table, pk_col)} from pipeline_config.yaml.

    Falls back to an empty dict (with a logged warning) if the config is
    missing or malformed, so a bad config degrades gracefully rather than
    crashing at import time.
    """
    try:
        cfg = get_config()

        schemas: dict = cfg.get("schemas", {})

        # Collect every file entry from both batch and stream expected_files
        # so we can map logical_name → DWH table name.
        all_expected: dict = {
            **cfg.get("watcher", {}).get("batch",  {}).get("expected_files", {}),
            **cfg.get("watcher", {}).get("stream", {}).get("expected_files", {}),
        }

        # reader.file_type_map gives us filename → logical_name
        file_type_map: dict[str, str] = cfg.get("reader", {}).get("file_type_map", {})

        # Invert to logical_name → filename (first match wins if duplicates)
        logical_to_filename: dict[str, str] = {}
        for filename, logical_name in file_type_map.items():
            if logical_name not in logical_to_filename:
                logical_to_filename[logical_name] = filename

        result: dict[str, tuple[str, str]] = {}

        for logical_name, schema_def in schemas.items():
            pk_col: str | None = schema_def.get("primary_key")
            if not pk_col:
                continue

            # Look up the DWH table name via filename → expected_files
            filename = logical_to_filename.get(logical_name)
            file_cfg = all_expected.get(filename, {}) if filename else {}
            table_raw: str | None = file_cfg.get("table")

            if not table_raw:
                # No table entry in config for this logical name — skip silently.
                # (e.g. cities are denormalised into dim_region and have no own entry)
                continue

            # Config uses "Dim_Customer", Postgres uses "dim_customer"
            postgres_table = table_raw.lower().replace("dim_", "dim_").replace("fact_", "fact_")
            # Normalise PascalCase like "Fact_Order" → "fact_order"
            postgres_table = table_raw.lower()

            result[logical_name] = (postgres_table, pk_col)

        if not result:
            logger.warning(
                "[fk_checker] FK_TABLE_MAP is empty — no schemas found in config. "
                "All FK lookups will be skipped."
            )

        return result

    except Exception as exc:
        logger.error(
            f"[fk_checker] Failed to build FK_TABLE_MAP from config: {exc}. "
            "Falling back to empty map — all FK checks will be skipped."
        )
        return {}


# Built once at module import — identical cost to the old hardcoded dict.
FK_TABLE_MAP: dict[str, tuple[str, str]] = _build_fk_table_map()


# ── Public API ────────────────────────────────────────────────────────────────

def get_valid_ids(table: str, pk_col: str) -> set:
    """
    Fetch all existing primary key values from a dimension or fact table.

    Returns a set of *normalised* strings (trailing ".0" stripped) so that
    the comparison in check_fk works correctly regardless of whether the
    source data arrived as an int, float, or string.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT "{pk_col}" FROM "{table}"')
        rows = cur.fetchall()
        cur.close()
        # Normalise every PK value so "370" and "370.0" both become "370"
        return {_normalize_id(row[0]) for row in rows}
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

    valid_ids = get_valid_ids(ref_table, ref_pk)

    # Normalise the FK column values before comparison so that "370.0" matches
    # the Postgres PK "370" (see _normalize_id for full explanation).
    normalised_fk = df[fk_col].apply(
        lambda v: _normalize_id(v) if pd.notna(v) else v
    )

    orphan_mask = df[fk_col].notna() & ~normalised_fk.isin(valid_ids)
    orphan_df   = df[orphan_mask].copy()
    clean_df    = df[~orphan_mask].copy()

    if not orphan_df.empty:
        logger.warning(
            f"[fk_checker] {len(orphan_df)} orphan(s) on '{fk_col}' "
            f"→ {ref_table}.{ref_pk}"
        )

    return clean_df, orphan_df


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