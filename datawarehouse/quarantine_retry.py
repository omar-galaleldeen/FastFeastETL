"""
quarantine_retry.py
===================
Retries quarantined records and promotes them into the appropriate
fact/dim table if they are now resolvable. Successfully loaded records
are deleted from quarantine. Records that still fail after the retry
window (24h) are promoted to true_orphan.

When to run
-----------
Call this at the START of each pipeline run (before the watchers start),
or schedule it as a standalone cron job every hour.

    python -m datawarehouse.quarantine_retry

What it does per rejection_stage
---------------------------------
schema   — Re-attempt was skipped intentionally: schema errors
           (bad dtype, null on NOT NULL column) are data-source problems
           that won't fix themselves. These are promoted directly to
           true_orphan after 24h so they don't retry forever.

records  — Same as schema: malformed data (bad email, duplicate, bad
           phone). Promoted to true_orphan after 24h.

orphan   — The interesting case. The parent record may have arrived in
           a later batch. Re-check each FK against the live DB. If all
           FKs now resolve → insert into the fact table → delete from
           quarantine. If still unresolved after 24h → true_orphan.

Retry strategy
--------------
  - Only orphan-stage records are retried against the DB.
  - schema/records records skip straight to the 24h true_orphan promotion.
  - A record is retried a maximum of MAX_RETRIES times regardless of age.
  - After 24h OR MAX_RETRIES attempts, unresolved records move to true_orphan.

Tables supported for retry
---------------------------
  fact_order        → checks customer_id, restaurant_id, driver_id
  fact_ticket       → checks order_id, (customer_id, driver_id, restaurant_id
                       are informational — order_id is the load-blocking FK)
  fact_ticket_event → checks ticket_id

Usage
-----
    # Retry everything
    python -m datawarehouse.quarantine_retry

    # Dry run (no writes, just report what would happen)
    python -m datawarehouse.quarantine_retry --dry-run

    # Retry only a specific table
    python -m datawarehouse.quarantine_retry --table fact_order

    # Override the true-orphan window (default 24h)
    python -m datawarehouse.quarantine_retry --orphan-hours 48

    OMAR: use this for testing:
        C:/ProgramData/anaconda3/python.exe -m datawarehouse.quarantine_retry --dry-run
    use this to retry just fact_order:
        C:/ProgramData/anaconda3/python.exe -m datawarehouse.quarantine_retry
"""


from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2.extras

from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MAX_RETRIES    = 5        # Give up after this many attempts regardless of age
ORPHAN_HOURS   = 24      # Promote to true_orphan after this many hours
BATCH_SIZE     = 500     # Records processed per DB round-trip

# ── FK map: source_table → list of (fk_column, referenced_table, ref_pk) ─────
# Only orphan-stage records need FK re-checking; schema/records go straight
# to the 24h window.
FK_MAP: dict[str, list[tuple[str, str, str]]] = {
    "fact_order": [
        ("customer_id",   "dim_customer",   "customer_id"),
        ("restaurant_id", "dim_restaurant", "restaurant_id"),
        ("driver_id",     "dim_driver",     "driver_id"),
    ],
    "fact_ticket": [
        ("order_id",      "fact_order",     "order_id"),
    ],
    "fact_ticket_event": [
        ("ticket_id",     "fact_ticket",    "ticket_id"),
    ],
}

# ── INSERT templates: source_table → (columns, pk_col, derived-field logic) ──
# Each entry is a callable that accepts the raw_data dict and returns a
# ready-to-insert tuple in column order, or None if the record is still
# uninsertable (e.g. a required field is missing even after FK resolution).

def _coerce(value: Any, typ: type, default=None):
    """Safe type coercion — returns default on None/NaN/error."""
    if value is None:
        return default
    s = str(value).strip()
    if s.lower() in ("nan", "none", "null", ""):
        return default
    try:
        return typ(s)
    except (ValueError, TypeError):
        return default


def _ts(value: Any):
    """Parse a timestamp string → datetime or None."""
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in ("nan", "none", "null", ""):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _date_key(ts: datetime | None) -> int | None:
    return int(ts.strftime("%Y%m%d")) if ts else None


def _time_key(ts: datetime | None) -> int | None:
    return int(ts.strftime("%H%M")) if ts else None


def _minutes_between(t1: datetime | None, t2: datetime | None) -> float | None:
    if t1 and t2:
        return round((t2 - t1).total_seconds() / 60, 2)
    return None


# ── fact_order ────────────────────────────────────────────────────────────────

_ORDER_COLS = [
    "order_id", "customer_id", "restaurant_id", "driver_id", "region_id",
    "date_key", "time_key", "order_status", "payment_method",
    "created_at", "delivered_at",
    "order_amount", "delivery_fee", "discount_amount", "total_amount",
]

def _build_order_row(d: dict) -> tuple | None:
    order_id = d.get("order_id")
    if not order_id:
        return None
    created = _ts(d.get("order_created_at") or d.get("created_at"))
    return (
        str(order_id),
        _coerce(d.get("customer_id"),   int),
        _coerce(d.get("restaurant_id"), int),
        _coerce(d.get("driver_id"),     int),
        _coerce(d.get("region_id"),     int),
        _date_key(created),
        _time_key(created),
        d.get("order_status"),
        d.get("payment_method"),
        created,
        _ts(d.get("delivered_at")),
        _coerce(d.get("order_amount"),    float),
        _coerce(d.get("delivery_fee"),    float),
        _coerce(d.get("discount_amount"), float),
        _coerce(d.get("total_amount"),    float),
    )


# ── fact_ticket ───────────────────────────────────────────────────────────────

_TICKET_COLS = [
    "ticket_id", "order_id", "customer_id", "region_id", "restaurant_id",
    "driver_id", "agent_id", "reason_id", "priority_id", "channel_id",
    "date_key", "time_key", "status", "refund_amount",
    "created_at", "first_response_at", "resolved_at",
    "sla_first_due_at", "sla_resolve_due_at",
    "first_response_min", "resolution_min",
    "sla_response_breached", "sla_resolution_breached", "reopened",
]

def _build_ticket_row(d: dict) -> tuple | None:
    ticket_id = d.get("ticket_id")
    if not ticket_id:
        return None
    created    = _ts(d.get("created_at"))
    first_resp = _ts(d.get("first_response_at"))
    resolved   = _ts(d.get("resolved_at"))
    sla_first  = _ts(d.get("sla_first_due_at"))
    sla_res    = _ts(d.get("sla_resolve_due_at"))

    sla_resp_breached = (first_resp > sla_first) if first_resp and sla_first else True
    sla_res_breached  = (resolved   > sla_res)   if resolved   and sla_res   else True
    reopened          = str(d.get("status", "")).strip() == "Reopened"

    return (
        str(ticket_id),
        d.get("order_id"),
        _coerce(d.get("customer_id"),   int),
        _coerce(d.get("region_id"),     int),
        _coerce(d.get("restaurant_id"), int),
        _coerce(d.get("driver_id"),     int),
        _coerce(d.get("agent_id"),      int),
        _coerce(d.get("reason_id"),     int),
        _coerce(d.get("priority_id"),   int),
        _coerce(d.get("channel_id"),    int),
        _date_key(created),
        _time_key(created),
        d.get("status"),
        _coerce(d.get("refund_amount"), float),
        created, first_resp, resolved, sla_first, sla_res,
        _minutes_between(created, first_resp),
        _minutes_between(created, resolved),
        sla_resp_breached,
        sla_res_breached,
        reopened,
    )


# ── fact_ticket_event ─────────────────────────────────────────────────────────

_EVENT_COLS = [
    "event_id", "ticket_id", "agent_id",
    "date_key", "time_key", "event_ts",
    "old_status", "new_status", "notes",
]

def _build_event_row(d: dict) -> tuple | None:
    event_id = d.get("event_id")
    if not event_id:
        return None
    event_ts = _ts(d.get("event_ts"))
    return (
        str(event_id),
        d.get("ticket_id"),
        _coerce(d.get("agent_id"), int),
        _date_key(event_ts),
        _time_key(event_ts),
        event_ts,
        d.get("old_status"),
        d.get("new_status"),
        d.get("notes"),
    )


# ── Dispatch tables ───────────────────────────────────────────────────────────

_COLS: dict[str, list[str]] = {
    "fact_order":        _ORDER_COLS,
    "fact_ticket":       _TICKET_COLS,
    "fact_ticket_event": _EVENT_COLS,
}

_ROW_BUILDERS: dict[str, callable] = {
    "fact_order":        _build_order_row,
    "fact_ticket":       _build_ticket_row,
    "fact_ticket_event": _build_event_row,
}

_PK_COLS: dict[str, str] = {
    "fact_order":        "order_id",
    "fact_ticket":       "ticket_id",
    "fact_ticket_event": "event_id",
}


# ── Core retry logic ──────────────────────────────────────────────────────────

def _fetch_valid_ids(conn, table: str, pk_col: str) -> set:
    """Return the full set of PKs currently in a table."""
    cur = conn.cursor()
    cur.execute(f'SELECT "{pk_col}" FROM "{table}"')
    rows = cur.fetchall()
    cur.close()
    return {str(r[0]) for r in rows}


def _normalize(v) -> str:
    """Strip trailing .0 from float-coerced integer strings."""
    s = str(v).strip()
    if s.endswith(".0"):
        try:
            return str(int(float(s)))
        except (ValueError, OverflowError):
            pass
    return s


def _all_fks_resolved(raw: dict, fk_list: list[tuple], valid_id_cache: dict) -> tuple[bool, str | None]:
    """
    Check all FK columns against the live-id cache.
    Returns (True, None) if all resolve, or (False, "col_name") for the
    first unresolved FK found.
    """
    for fk_col, ref_table, ref_pk in fk_list:
        val = raw.get(fk_col)
        if val is None or str(val).strip().lower() in ("nan", "none", "null", ""):
            continue  # nullable FK — skip check
        normalized = _normalize(val)
        if normalized not in valid_id_cache.get(ref_table, set()):
            return False, fk_col
    return True, None


def _insert_rows(conn, table: str, columns: list[str], rows: list[tuple], pk_col: str) -> int:
    """Bulk-insert rows into a fact table. Returns count inserted."""
    if not rows:
        return 0
    col_list    = ", ".join(f'"{c}"' for c in columns)
    placeholder = ", ".join(["%s"] * len(columns))
    sql = (
        f'INSERT INTO "{table}" ({col_list}) '
        f'VALUES ({placeholder}) '
        f'ON CONFLICT ("{pk_col}") DO NOTHING'
    )
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=BATCH_SIZE)
    count = cur.rowcount if cur.rowcount >= 0 else len(rows)
    cur.close()
    return count


def _promote_to_true_orphan(conn, records: list[dict], unresolved_fk: str) -> None:
    """Move records that exceeded the retry window into true_orphan."""
    if not records:
        return
    sql = """
        INSERT INTO true_orphan
            (quarantine_id, source_file, source_table, unresolved_fk, raw_data)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT DO NOTHING
    """
    rows = [
        (
            r["quarantine_id"],
            r["source_file"],
            r["source_table"],
            unresolved_fk,
            json.dumps(r["raw_data"], default=str),
        )
        for r in records
    ]
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=BATCH_SIZE)
    cur.close()


def _delete_from_quarantine(conn, quarantine_ids: list[int]) -> None:
    """Delete successfully retried records from quarantine."""
    if not quarantine_ids:
        return
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM quarantine WHERE quarantine_id = ANY(%s)",
        (quarantine_ids,)
    )
    cur.close()


# ── Main retry runner ─────────────────────────────────────────────────────────

def run_retry(
    target_table: str | None = None,
    dry_run: bool = False,
    orphan_hours: int = ORPHAN_HOURS,
) -> dict:
    """
    Main entry point. Processes all eligible quarantine records.

    Parameters
    ----------
    target_table : if set, only process records for this source_table.
    dry_run      : if True, log what would happen but write nothing.
    orphan_hours : records older than this many hours with unresolved FKs
                   are promoted to true_orphan.

    Returns
    -------
    dict with keys: retried, promoted, deleted, skipped, errors
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=orphan_hours)

    stats = {
        "retried":   0,   # orphan records that were successfully re-inserted
        "promoted":  0,   # records moved to true_orphan
        "deleted":   0,   # records removed from quarantine after success
        "skipped":   0,   # schema/records stage — not retried
        "errors":    0,   # unexpected failures
    }

    conn = get_conn()
    try:
        # ── 1. Fetch quarantine records ───────────────────────────────────── #
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
            SELECT
                quarantine_id,
                source_file,
                source_table,
                rejection_stage,
                raw_data,
                quarantined_at
            FROM quarantine
            WHERE 1=1
        """
        params = []
        if target_table:
            query += " AND source_table = %s"
            params.append(target_table)
        query += " ORDER BY quarantined_at ASC"

        cur.execute(query, params or None)
        all_records = cur.fetchall()
        cur.close()

        if not all_records:
            logger.info("[quarantine_retry] Quarantine is empty — nothing to retry.")
            return stats

        logger.info(f"[quarantine_retry] Found {len(all_records)} quarantine record(s) to evaluate.")

        # ── Diagnostic: show breakdown of what was fetched ────────────────── #
        from collections import Counter
        breakdown = Counter(
            (str(row["source_table"] or "NULL"), row["rejection_stage"])
            for row in all_records
        )
        print("\n[DIAGNOSTIC] Records fetched from quarantine:")
        for (tbl, stage), cnt in sorted(breakdown.items()):
            print(f"  source_table={tbl!r:30s}  stage={stage!r:10s}  count={cnt}")
        print()

        # ── 2. Group by source_table and rejection_stage ──────────────────── #
        # {source_table: {"orphan": [...], "schema": [...], "records": [...]}}
        grouped: dict[str, dict[str, list]] = {}
        for row in all_records:
            tbl   = row["source_table"] or "unknown"
            stage = row["rejection_stage"]
            grouped.setdefault(tbl, {}).setdefault(stage, []).append(dict(row))

        # ── 3. Build live FK id cache for all referenced tables ───────────── #
        # One query per referenced table — cheap and avoids N+1 lookups.
        valid_id_cache: dict[str, set] = {}
        all_fk_refs = set()
        for tbl in grouped:
            for fk_col, ref_table, ref_pk in FK_MAP.get(tbl, []):
                all_fk_refs.add((ref_table, ref_pk))

        for ref_table, ref_pk in all_fk_refs:
            try:
                valid_id_cache[ref_table] = _fetch_valid_ids(conn, ref_table, ref_pk)
                logger.info(
                    f"[quarantine_retry] Cached {len(valid_id_cache[ref_table])} "
                    f"ids from {ref_table}.{ref_pk}"
                )
            except Exception as exc:
                logger.error(f"[quarantine_retry] Could not cache ids from {ref_table}: {exc}")
                valid_id_cache[ref_table] = set()

        # ── 4. Process each group ─────────────────────────────────────────── #
        for source_table, stage_map in grouped.items():

            row_builder = _ROW_BUILDERS.get(source_table)
            columns     = _COLS.get(source_table)
            pk_col      = _PK_COLS.get(source_table)
            fk_list     = FK_MAP.get(source_table, [])

            # ── 4a. schema / records stage: skip retry, check age ─────────── #
            for stage in ("schema", "records"):
                bad_records = stage_map.get(stage, [])
                if not bad_records:
                    continue

                stats["skipped"] += len(bad_records)
                promote_ids   = []
                promote_recs  = []

                for rec in bad_records:
                    quarantined_at = rec["quarantined_at"]
                    if quarantined_at.tzinfo is None:
                        quarantined_at = quarantined_at.replace(tzinfo=timezone.utc)

                    if quarantined_at < cutoff:
                        promote_ids.append(rec["quarantine_id"])
                        promote_recs.append(rec)

                if promote_recs:
                    logger.info(
                        f"[quarantine_retry] {source_table}/{stage}: promoting "
                        f"{len(promote_recs)} aged record(s) → true_orphan"
                    )
                    if not dry_run:
                        with conn:
                            _promote_to_true_orphan(conn, promote_recs, stage)
                            _delete_from_quarantine(conn, promote_ids)
                        stats["promoted"] += len(promote_recs)
                        stats["deleted"]  += len(promote_ids)

            # ── 4b. orphan stage: re-check FKs, attempt insert ────────────── #
            orphan_records = stage_map.get("orphan", [])
            if not orphan_records:
                continue

            if row_builder is None:
                logger.warning(
                    f"[quarantine_retry] No row builder for '{source_table}' — "
                    f"skipping {len(orphan_records)} orphan record(s)."
                )
                stats["skipped"] += len(orphan_records)
                continue

            resolved_rows    = []   # tuples ready to INSERT
            resolved_ids     = []   # quarantine_ids to DELETE on success
            still_orphan     = []   # records with FKs still missing
            aged_out_recs    = []   # still_orphan AND past cutoff
            aged_out_ids     = []

            for rec in orphan_records:
                raw            = rec["raw_data"]
                quarantined_at = rec["quarantined_at"]
                if quarantined_at.tzinfo is None:
                    quarantined_at = quarantined_at.replace(tzinfo=timezone.utc)

                resolved, unresolved_fk = _all_fks_resolved(raw, fk_list, valid_id_cache)

                if resolved:
                    # Try to build the insert row
                    row = row_builder(raw)
                    if row is not None:
                        resolved_rows.append(row)
                        resolved_ids.append(rec["quarantine_id"])
                    else:
                        # Row builder returned None — uninsertable (missing PK etc)
                        logger.warning(
                            f"[quarantine_retry] {source_table}: FK resolved but row "
                            f"builder returned None for quarantine_id={rec['quarantine_id']}. "
                            "Skipping."
                        )
                        stats["skipped"] += 1
                else:
                    still_orphan.append((rec, unresolved_fk or "unknown"))
                    if quarantined_at < cutoff:
                        aged_out_recs.append((rec, unresolved_fk or "unknown"))
                        aged_out_ids.append(rec["quarantine_id"])

            # Insert resolved records
            if resolved_rows:
                logger.info(
                    f"[quarantine_retry] {source_table}: attempting to insert "
                    f"{len(resolved_rows)} resolved record(s)"
                )
                if not dry_run:
                    try:
                        with conn:
                            inserted = _insert_rows(conn, source_table, columns, resolved_rows, pk_col)
                            _delete_from_quarantine(conn, resolved_ids)
                        stats["retried"] += inserted
                        stats["deleted"] += len(resolved_ids)
                        logger.info(
                            f"[quarantine_retry] {source_table}: inserted {inserted} "
                            f"and deleted {len(resolved_ids)} from quarantine ✓"
                        )
                    except Exception as exc:
                        stats["errors"] += len(resolved_rows)
                        logger.error(
                            f"[quarantine_retry] {source_table}: insert failed: {exc}"
                        )
                else:
                    logger.info(
                        f"[quarantine_retry] DRY RUN — would insert {len(resolved_rows)} "
                        f"rows into {source_table} and delete {len(resolved_ids)} from quarantine."
                    )
                    stats["retried"] += len(resolved_rows)

            # Log still-unresolved summary
            if still_orphan:
                logger.info(
                    f"[quarantine_retry] {source_table}: {len(still_orphan)} record(s) "
                    f"still have unresolved FKs."
                )

            # Promote aged-out records to true_orphan
            if aged_out_recs:
                logger.info(
                    f"[quarantine_retry] {source_table}: promoting {len(aged_out_recs)} "
                    f"aged-out record(s) → true_orphan"
                )
                if not dry_run:
                    by_fk: dict[str, list] = {}
                    by_fk_ids: dict[str, list] = {}
                    for rec, fk in aged_out_recs:
                        by_fk.setdefault(fk, []).append(rec)
                        by_fk_ids.setdefault(fk, []).append(rec["quarantine_id"])

                    try:
                        with conn:
                            for fk, recs in by_fk.items():
                                _promote_to_true_orphan(conn, recs, fk)
                            all_aged_ids = [i for ids in by_fk_ids.values() for i in ids]
                            _delete_from_quarantine(conn, all_aged_ids)
                        stats["promoted"] += len(aged_out_recs)
                        stats["deleted"]  += len(aged_out_recs)
                    except Exception as exc:
                        stats["errors"] += len(aged_out_recs)
                        logger.error(
                            f"[quarantine_retry] true_orphan promotion failed: {exc}"
                        )

    except Exception as exc:
        logger.error(f"[quarantine_retry] Unexpected error: {exc}", exc_info=True)
        stats["errors"] += 1
    finally:
        put_conn(conn)

    # ── 5. Summary ────────────────────────────────────────────────────────── #
    logger.info(
        f"[quarantine_retry] Done — "
        f"retried={stats['retried']} | "
        f"promoted={stats['promoted']} | "
        f"deleted={stats['deleted']} | "
        f"skipped={stats['skipped']} | "
        f"errors={stats['errors']}"
    )
    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Retry quarantined records and promote aged-out ones to true_orphan."
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Only process records for this source_table (e.g. fact_order)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would happen without writing anything to the DB.",
    )
    parser.add_argument(
        "--orphan-hours",
        type=int,
        default=ORPHAN_HOURS,
        help=f"Hours before an unresolved orphan is promoted to true_orphan (default: {ORPHAN_HOURS})",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("FastFeast Quarantine Retry")
    print("=" * 60)
    if args.dry_run:
        print("⚠  DRY RUN — no changes will be written\n")

    # When run standalone (not via ingestion_runner), the connection pool
    # has not been initialised yet. Boot it now — init_pool() reads
    # pipeline_config.yaml itself, so no arguments needed.
    from datawarehouse.db_connection import init_pool
    init_pool()

    stats = run_retry(
        target_table=args.table,
        dry_run=args.dry_run,
        orphan_hours=args.orphan_hours,
    )

    print("\n[RESULTS]")
    print(f"  Successfully retried & loaded : {stats['retried']}")
    print(f"  Deleted from quarantine       : {stats['deleted']}")
    print(f"  Promoted to true_orphan       : {stats['promoted']}")
    print(f"  Skipped (schema/records stage): {stats['skipped']}")
    print(f"  Errors                        : {stats['errors']}")
    print("=" * 60)


if __name__ == "__main__":
    main()