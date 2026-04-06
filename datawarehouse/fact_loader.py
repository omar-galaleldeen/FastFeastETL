"""
fact_loader.py
==============
Loads validated stream DataFrames into fact tables.
SLA breach flags and resolution times are calculated here before INSERT
so the DWH already contains analytics-ready columns.

Pure INSERT, no upsert — validation layer guarantees uniqueness.
ON CONFLICT DO NOTHING as safety net only.
"""
from __future__ import annotations

import pandas as pd
import psycopg2.extras

from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe(df: pd.DataFrame, col: str) -> list:
    if col in df.columns:
        return df[col].where(pd.notna(df[col]), None).tolist()
    return [None] * len(df)


def _date_key(ts_series: pd.Series) -> list:
    """Convert a datetime series to YYYYMMDD integer keys for dim_date."""
    return [
        int(ts.strftime("%Y%m%d")) if pd.notna(ts) else None
        for ts in ts_series
    ]


def _time_key(ts_series: pd.Series) -> list:
    """Convert a datetime series to HHMM integer keys for dim_time."""
    return [
        int(ts.strftime("%H%M")) if pd.notna(ts) else None
        for ts in ts_series
    ]


def _minutes_between(t1_series: pd.Series, t2_series: pd.Series) -> list:
    """Return elapsed minutes between two timestamp series, or None."""
    result = []
    for t1, t2 in zip(t1_series, t2_series):
        if pd.notna(t1) and pd.notna(t2):
            diff = (t2 - t1).total_seconds() / 60
            result.append(round(diff, 2))
        else:
            result.append(None)
    return result


def _insert(table: str, columns: list[str], rows: list[tuple], pk_col: str) -> int:
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
        logger.info(f"[fact_loader] Inserted {len(rows)} rows → {table}")
        return len(rows)
    except Exception as exc:
        logger.error(f"[fact_loader] Insert failed for {table}: {exc}")
        raise
    finally:
        put_conn(conn)


# ── Fact loaders ──────────────────────────────────────────────────────────────

def load_orders(df: pd.DataFrame) -> int:
    """Load fact_order. Derives date_key and time_key from order_created_at."""
    if df.empty:
        return 0

    # Ensure datetime columns are parsed
    created = pd.to_datetime(df.get("order_created_at"), errors="coerce", utc=False)

    rows = list(zip(
        _safe(df, "order_id"),
        _safe(df, "customer_id"),
        _safe(df, "restaurant_id"),
        _safe(df, "driver_id"),
        _safe(df, "region_id"),
        _date_key(created),
        _time_key(created),
        _safe(df, "order_status"),
        _safe(df, "payment_method"),
        created.where(pd.notna(created), None).tolist(),
        _safe(df, "delivered_at"),
        _safe(df, "order_amount"),
        _safe(df, "delivery_fee"),
        _safe(df, "discount_amount"),
        _safe(df, "total_amount"),
    ))

    return _insert(
        table   = "fact_order",
        columns = ["order_id","customer_id","restaurant_id","driver_id","region_id",
                   "date_key","time_key","order_status","payment_method",
                   "created_at","delivered_at","order_amount","delivery_fee",
                   "discount_amount","total_amount"],
        rows    = rows,
        pk_col  = "order_id",
    )


def load_tickets(df: pd.DataFrame) -> int:
    """
    Load fact_ticket.
    Calculates:
      - first_response_min  : minutes from created_at → first_response_at
      - resolution_min      : minutes from created_at → resolved_at
      - sla_response_breached : actual > sla_first_due_at
      - sla_resolution_breached : actual > sla_resolve_due_at
      - reopened            : status == 'Reopened'
    """
    if df.empty:
        return 0

    created      = pd.to_datetime(_safe(df, "created_at"),         errors="coerce")
    first_resp   = pd.to_datetime(_safe(df, "first_response_at"),  errors="coerce")
    resolved     = pd.to_datetime(_safe(df, "resolved_at"),        errors="coerce")
    sla_first_due= pd.to_datetime(_safe(df, "sla_first_due_at"),   errors="coerce")
    sla_res_due  = pd.to_datetime(_safe(df, "sla_resolve_due_at"), errors="coerce")

    # SLA breach flags (calculated in pipeline as spec requires)
    sla_resp_breached = [
        (fr > sfd) if pd.notna(fr) and pd.notna(sfd) else True
        for fr, sfd in zip(first_resp, sla_first_due)
    ]
    sla_res_breached = [
        (res > srd) if pd.notna(res) and pd.notna(srd) else True
        for res, srd in zip(resolved, sla_res_due)
    ]

    reopened_flags = [
        str(s).strip() == "Reopened" if s is not None else False
        for s in _safe(df, "status")
    ]

    rows = list(zip(
        _safe(df, "ticket_id"),
        _safe(df, "order_id"),
        _safe(df, "customer_id"),
        _safe(df, "region_id"),
        _safe(df, "restaurant_id"),
        _safe(df, "driver_id"),
        _safe(df, "agent_id"),
        _safe(df, "reason_id"),
        _safe(df, "priority_id"),
        _safe(df, "channel_id"),
        _date_key(pd.Series(created)),
        _time_key(pd.Series(created)),
        _safe(df, "status"),
        _safe(df, "refund_amount"),
        created.tolist(),
        first_resp.tolist(),
        resolved.tolist(),
        sla_first_due.tolist(),
        sla_res_due.tolist(),
        _minutes_between(pd.Series(created), pd.Series(first_resp)),
        _minutes_between(pd.Series(created), pd.Series(resolved)),
        sla_resp_breached,
        sla_res_breached,
        reopened_flags,
    ))

    return _insert(
        table   = "fact_ticket",
        columns = ["ticket_id","order_id","customer_id","region_id","restaurant_id",
                   "driver_id","agent_id","reason_id","priority_id","channel_id",
                   "date_key","time_key","status","refund_amount",
                   "created_at","first_response_at","resolved_at",
                   "sla_first_due_at","sla_resolve_due_at",
                   "first_response_min","resolution_min",
                   "sla_response_breached","sla_resolution_breached","reopened"],
        rows    = rows,
        pk_col  = "ticket_id",
    )


def load_ticket_events(df: pd.DataFrame) -> int:
    """Load fact_ticket_event. Derives date_key and time_key from event_ts."""
    if df.empty:
        return 0

    event_ts = pd.to_datetime(_safe(df, "event_ts"), errors="coerce")

    rows = list(zip(
        _safe(df, "event_id"),
        _safe(df, "ticket_id"),
        _safe(df, "agent_id"),
        _date_key(pd.Series(event_ts)),
        _time_key(pd.Series(event_ts)),
        event_ts.tolist(),
        _safe(df, "old_status"),
        _safe(df, "new_status"),
        _safe(df, "notes"),
    ))

    return _insert(
        table   = "fact_ticket_event",
        columns = ["event_id","ticket_id","agent_id","date_key","time_key",
                   "event_ts","old_status","new_status","notes"],
        rows    = rows,
        pk_col  = "event_id",
    )


# ── Dispatcher ────────────────────────────────────────────────────────────────

FACT_LOADERS: dict[str, callable] = {
    "orders":         load_orders,
    "tickets":        load_tickets,
    "ticket_events":  load_ticket_events,
}


def load_fact(file_name: str, df: pd.DataFrame) -> int:
    """
    Dispatch to the correct fact loader.

    Parameters
    ----------
    file_name : logical name without extension, e.g. "orders"
    df        : validated, PII-masked DataFrame

    Returns
    -------
    Number of rows inserted (0 if no loader found)
    """
    loader = FACT_LOADERS.get(file_name)
    if loader is None:
        logger.warning(f"[fact_loader] No loader registered for '{file_name}'")
        return 0
    return loader(df)
