"""
dim_loader.py
=============
Loads validated dimension DataFrames into their Postgres tables.
Uses psycopg2 executemany for maximum throughput — no ORM overhead.

Design:
  - Pure INSERT (no upsert) — validation layer guarantees no duplicates
  - ON CONFLICT DO NOTHING as a safety net only (idempotency guard)
  - Column mapping defined here — maps source column names to DWH column names
  - Each dimension has its own load function for clarity
"""
from __future__ import annotations

import pandas as pd
import psycopg2.extras

from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Generic insert helper ─────────────────────────────────────────────────────

def _insert(
    table:   str,
    columns: list[str],
    rows:    list[tuple],
    pk_col:  str,
) -> int:
    """
    Execute a batch INSERT INTO table (columns) VALUES (...) ON CONFLICT DO NOTHING.

    Returns the number of rows actually inserted.
    """
    if not rows:
        return 0

    col_list    = ", ".join(f'"{c}"' for c in columns)
    placeholder = ", ".join(["%s"] * len(columns))
    sql         = (
        f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholder}) '
        f'ON CONFLICT ("{pk_col}") DO NOTHING'
    )

    conn = get_conn()
    try:
        with conn:
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
            cur.close()
        logger.info(f"[dim_loader] Inserted {len(rows)} rows → {table}")
        return len(rows)
    except Exception as exc:
        logger.error(f"[dim_loader] Insert failed for {table}: {exc}")
        raise
    finally:
        put_conn(conn)


def _safe(df: pd.DataFrame, col: str):
    """Return column values as a list, or None list if column doesn't exist."""
    if col in df.columns:
        return df[col].where(pd.notna(df[col]), None).tolist()
    return [None] * len(df)


# ── Per-dimension loaders ─────────────────────────────────────────────────────

def load_customers(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "customer_id"),
        _safe(df, "full_name"),
        _safe(df, "email"),        # already hashed by pii_handler
        _safe(df, "phone"),        # already hashed by pii_handler
        _safe(df, "region_id"),
        _safe(df, "segment_id"),
        _safe(df, "signup_date"),
        _safe(df, "gender"),
        _safe(df, "created_at"),
        _safe(df, "updated_at"),
    ))
    return _insert(
        table   = "dim_customer",
        columns = ["customer_id","full_name","email","phone","region_id",
                   "segment_id","signup_date","gender","created_at","updated_at"],
        rows    = rows,
        pk_col  = "customer_id",
    )


def load_drivers(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "driver_id"),
        _safe(df, "driver_name"),
        _safe(df, "driver_phone"),     # already hashed by pii_handler
        _safe(df, "national_id"),      # already hashed by pii_handler
        _safe(df, "region_id"),
        _safe(df, "shift"),
        _safe(df, "vehicle_type"),
        _safe(df, "hire_date"),
        _safe(df, "rating_avg"),
        _safe(df, "on_time_rate"),
        _safe(df, "cancel_rate"),
        _safe(df, "completed_deliveries"),
        _safe(df, "is_active"),
        _safe(df, "created_at"),
        _safe(df, "updated_at"),
    ))
    return _insert(
        table   = "dim_driver",
        columns = ["driver_id","driver_name","driver_phone","national_id","region_id",
                   "shift","vehicle_type","hire_date","rating_avg","on_time_rate",
                   "cancel_rate","completed_deliveries","is_active","created_at","updated_at"],
        rows    = rows,
        pk_col  = "driver_id",
    )


def load_restaurants(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "restaurant_id"),
        _safe(df, "restaurant_name"),
        _safe(df, "region_id"),
        _safe(df, "category_id"),
        _safe(df, "price_tier"),
        _safe(df, "rating_avg"),
        _safe(df, "prep_time_avg_min"),
        _safe(df, "is_active"),
        _safe(df, "created_at"),
        _safe(df, "updated_at"),
    ))
    return _insert(
        table   = "dim_restaurant",
        columns = ["restaurant_id","restaurant_name","region_id","category_id",
                   "price_tier","rating_avg","prep_time_avg_min","is_active",
                   "created_at","updated_at"],
        rows    = rows,
        pk_col  = "restaurant_id",
    )


def load_agents(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "agent_id"),
        _safe(df, "agent_name"),
        _safe(df, "agent_email"),    # already hashed by pii_handler
        _safe(df, "agent_phone"),    # already hashed by pii_handler
        _safe(df, "team_id"),
        _safe(df, "skill_level"),
        _safe(df, "hire_date"),
        _safe(df, "avg_handle_time_min"),
        _safe(df, "resolution_rate"),
        _safe(df, "csat_score"),
        _safe(df, "is_active"),
        _safe(df, "created_at"),
        _safe(df, "updated_at"),
    ))
    return _insert(
        table   = "dim_agent",
        columns = ["agent_id","agent_name","agent_email","agent_phone","team_id",
                   "skill_level","hire_date","avg_handle_time_min","resolution_rate",
                   "csat_score","is_active","created_at","updated_at"],
        rows    = rows,
        pk_col  = "agent_id",
    )


def load_regions(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "region_id"),
        _safe(df, "region_name"),
        _safe(df, "delivery_base_fee"),
    ))
    return _insert(
        table   = "dim_region",
        columns = ["region_id","region_name","delivery_base_fee"],
        rows    = rows,
        pk_col  = "region_id",
    )


def load_categories(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "category_id"),
        _safe(df, "category_name"),
    ))
    return _insert(
        table   = "dim_category",
        columns = ["category_id","category_name"],
        rows    = rows,
        pk_col  = "category_id",
    )


def load_segments(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "segment_id"),
        _safe(df, "segment_name"),
        _safe(df, "discount_pct"),
        _safe(df, "priority_support"),
    ))
    return _insert(
        table   = "dim_segment",
        columns = ["segment_id","segment_name","discount_pct","priority_support"],
        rows    = rows,
        pk_col  = "segment_id",
    )


def load_teams(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "team_id"),
        _safe(df, "team_name"),
    ))
    return _insert(
        table   = "dim_team",
        columns = ["team_id","team_name"],
        rows    = rows,
        pk_col  = "team_id",
    )


def load_channels(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "channel_id"),
        _safe(df, "channel_name"),
    ))
    return _insert(
        table   = "dim_channel",
        columns = ["channel_id","channel_name"],
        rows    = rows,
        pk_col  = "channel_id",
    )


def load_priorities(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "priority_id"),
        _safe(df, "priority_code"),
        _safe(df, "priority_name"),
        _safe(df, "sla_first_response_min"),
        _safe(df, "sla_resolution_min"),
    ))
    return _insert(
        table   = "dim_priority",
        columns = ["priority_id","priority_code","priority_name",
                   "sla_first_response_min","sla_resolution_min"],
        rows    = rows,
        pk_col  = "priority_id",
    )


def load_reasons(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "reason_id"),
        _safe(df, "reason_name"),
        _safe(df, "reason_category_id"),
        _safe(df, "severity_level"),
        _safe(df, "typical_refund_pct"),
    ))
    return _insert(
        table   = "dim_reason",
        columns = ["reason_id","reason_name","reason_category_id",
                   "severity_level","typical_refund_pct"],
        rows    = rows,
        pk_col  = "reason_id",
    )


def load_reason_categories(df: pd.DataFrame) -> int:
    rows = list(zip(
        _safe(df, "reason_category_id"),
        _safe(df, "category_name"),
    ))
    return _insert(
        table   = "dim_reason_category",
        columns = ["reason_category_id","category_name"],
        rows    = rows,
        pk_col  = "reason_category_id",
    )


def load_cities(df: pd.DataFrame) -> int:
    """
    Cities are denormalised into dim_region — we just log and skip.
    The region loader handles the city_name column from cities.json.
    """
    logger.info(f"[dim_loader] cities.json received ({len(df)} rows) — "
                "city data is denormalised into dim_region, skipping direct load.")
    return 0


# ── Dispatcher ────────────────────────────────────────────────────────────────
# Maps logical file name → loader function
# Used by ingestion_runner step 4.5

DIM_LOADERS: dict[str, callable] = {
    "customers":        load_customers,
    "drivers":          load_drivers,
    "restaurants":      load_restaurants,
    "agents":           load_agents,
    "regions":          load_regions,
    "categories":       load_categories,
    "segments":         load_segments,
    "teams":            load_teams,
    "channels":         load_channels,
    "priorities":       load_priorities,
    "reasons":          load_reasons,
    "reason_categories":load_reason_categories,
    "cities":           load_cities,
}


def load_dimension(file_name: str, df: pd.DataFrame) -> int:
    """
    Dispatch to the correct dimension loader based on file_name.

    Parameters
    ----------
    file_name : logical name without extension, e.g. "customers"
    df        : validated, PII-masked DataFrame

    Returns
    -------
    Number of rows inserted (0 if no loader found)
    """
    loader = DIM_LOADERS.get(file_name)
    if loader is None:
        logger.warning(f"[dim_loader] No loader registered for '{file_name}'")
        return 0
    return loader(df)
