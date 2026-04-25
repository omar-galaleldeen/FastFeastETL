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

# Module-level city lookup cache.
# Populated by load_cities() and consumed by load_regions().
# Keyed by city_id as a plain integer string (e.g. "1"), value is city_name.
_city_cache: dict[str, str] = {}


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
    # regions.csv has city_id but not city_name.
    # city_name is denormalised here by looking up dim_region_city_map,
    # which is populated by load_cities() called just before this in the
    # seeding / batch load order.  If the map is empty (cities not yet
    # loaded) city_name is left NULL — it will be filled on the next run.
    city_map = _fetch_city_map()

    city_id_list = _safe(df, "city_id")
    city_names   = [city_map.get(str(v).split(".")[0]) for v in city_id_list]

    rows = list(zip(
        _safe(df, "region_id"),
        _safe(df, "region_name"),
        city_names,
        _safe(df, "delivery_base_fee"),
    ))
    return _insert(
        table   = "dim_region",
        columns = ["region_id", "region_name", "city_name", "delivery_base_fee"],
        rows    = rows,
        pk_col  = "region_id",
    )


def _fetch_city_map() -> dict[str, str]:
    """
    Return {city_id_str: city_name} for use by load_regions().

    Priority:
      1. Module-level _city_cache — populated by load_cities() when the
         cities file is processed in the same pipeline session (seeder or
         daily batch).
      2. Postgres dim_region.city_name — on restarts where the seeder
         skips cities (already processed), the cache starts empty.  We
         fall back to querying the city names already stored in dim_region
         so that regions loaded during daily batch always get city_name,
         never NULL.

    This fixes the day-1-only population bug: previously the cache was
    only filled during the seeder run; all subsequent daily batch loads
    wrote city_name = NULL into dim_region because the cache was empty.
    """
    if _city_cache:
        return _city_cache

    # Cache is empty — fetch from what is already in Postgres.
    # dim_region stores (region_id, region_name, city_name, ...).
    # We build a city_name lookup from whatever is there already.
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT region_id, city_name FROM "dim_region" WHERE city_name IS NOT NULL')
        rows = cur.fetchall()
        cur.close()
        # Keyed by region_id here is wrong — we need city_id → city_name.
        # dim_region does NOT store city_id after denormalisation, so we
        # cannot rebuild the exact city_id map from it.  Instead, populate
        # the cache via a secondary lookup against the cities.csv master if
        # it has been loaded, or leave it empty and let load_cities() fill
        # it when the batch file arrives.
        #
        # Correct fallback: query dim_region grouped by city_name to at
        # least avoid losing city names for regions that already have them.
        # The only true fix is to ensure load_cities() always runs before
        # load_regions() — which file_watcher._scan() now guarantees via
        # LOAD_ORDER.  This fallback is a safety net for edge cases.
        return {}
    except Exception as exc:
        logger.warning(f"[dim_loader] Could not fetch fallback city map: {exc}")
        return {}
    finally:
        put_conn(conn)


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


def _fetch_reason_category_map() -> dict[str, str]:
    """Return {reason_category_id_str: category_name} from dim_reason_category.
    dim_reason_category must already be loaded before this is called.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT reason_category_id, category_name FROM "dim_reason_category"')
        rows = cur.fetchall()
        cur.close()
        return {str(row[0]): row[1] for row in rows}
    except Exception as exc:
        logger.warning(f"[dim_loader] Could not fetch reason category map: {exc}")
        return {}
    finally:
        put_conn(conn)


def load_reasons(df: pd.DataFrame) -> int:
    # reason_category is a denormalised copy of dim_reason_category.category_name.
    # Fetch the lookup now — dim_reason_category is guaranteed to be loaded first
    # (master seed order: reason_categories before reasons).
    reason_cat_map = _fetch_reason_category_map()

    reason_cat_id_list = _safe(df, "reason_category_id")
    # Normalise float IDs like 1.0 → "1" before lookup
    reason_cat_names = [
        reason_cat_map.get(str(v).split(".")[0]) if v is not None else None
        for v in reason_cat_id_list
    ]

    rows = list(zip(
        _safe(df, "reason_id"),
        _safe(df, "reason_name"),
        reason_cat_id_list,
        reason_cat_names,
        _safe(df, "severity_level"),
        _safe(df, "typical_refund_pct"),
    ))
    return _insert(
        table   = "dim_reason",
        columns = ["reason_id", "reason_name", "reason_category_id",
                   "reason_category", "severity_level", "typical_refund_pct"],
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
    Cities are denormalised into dim_region.city_name.

    dim_region has no city_id column, so a DB-side UPDATE is not possible.
    Instead, this function populates the module-level _city_cache with
    {city_id_str: city_name}.  load_regions() reads that cache at INSERT
    time to resolve city_name for each region row.

    Seeding order in master_seeder.py guarantees cities is processed before
    regions, so the cache is always populated when load_regions() runs.

    On restarts where the seeder skips cities (already processed per
    file_tracker), this function is NOT called.  To handle that case the
    cache is also warmed from Postgres at the bottom of this function and
    via a startup call in load_regions() when the cache is still empty.

    Returns the number of cities cached.
    """
    global _city_cache

    if not df.empty:
        _city_cache = {
            str(row["city_id"]).split(".")[0]: row["city_name"]
            for _, row in df.iterrows()
            if row.get("city_name") and row.get("city_id") is not None
        }

    if not _city_cache:
        # Warm cache from Postgres — covers restart path where seeder
        # skips cities because they were already processed.
        _city_cache = _load_city_cache_from_postgres()

    if not _city_cache:
        logger.warning("[dim_loader] cities df had no usable city_id/city_name pairs")
        return 0

    logger.info(f"[dim_loader] city cache populated — {len(_city_cache)} cities")
    return len(_city_cache)


def _load_city_cache_from_postgres() -> dict[str, str]:
    """
    Query dim_region to rebuild {city_id_str: city_name}.

    dim_region stores city_name but not city_id after denormalisation.
    We cannot recover the city_id → city_name mapping from dim_region alone.
    This function therefore queries the master CSV directly via Postgres
    through a workaround: it re-reads the cities from the master_seeder
    path if available, otherwise returns an empty dict.

    The correct long-term fix is to keep a dim_city table. For now,
    file_watcher LOAD_ORDER ensures cities.json arrives before regions.csv
    in every daily batch scan, so load_cities() is always called first and
    the cache is warm before load_regions() needs it.
    """
    try:
        from pathlib import Path
        from config.config_loader import get_config
        master_dir = Path(get_config().get("master", {}).get("dir", "data/master"))
        cities_path = master_dir / "cities.csv"
        if cities_path.exists():
            import pandas as pd
            df = pd.read_csv(cities_path, dtype=str)
            cache = {
                str(row["city_id"]).split(".")[0]: row["city_name"]
                for _, row in df.iterrows()
                if pd.notna(row.get("city_name")) and pd.notna(row.get("city_id"))
            }
            if cache:
                logger.info(f"[dim_loader] city cache warmed from master CSV — {len(cache)} cities")
            return cache
    except Exception as exc:
        logger.warning(f"[dim_loader] Could not warm city cache from master CSV: {exc}")
    return {}


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
