"""
schema_init.py
==============
Creates all tables in fastfeastapp on pipeline startup.
Uses IF NOT EXISTS everywhere — safe to call on every run.

Key design choices:
  - FK REFERENCES enforced on all fact and dimension tables for
    correct JOIN behaviour and query planner optimisation
  - orphan_validator ensures no row reaches a fact table with a
    missing parent — FK constraints are the final safety net
  - No CHECK constraints on business columns (validation layer owns that)
  - Quarantine table stores raw_data as JSONB — works for any table shape
  - pipeline_file_log is the analytics-facing file tracking record
"""
from __future__ import annotations

from datawarehouse.db_connection import get_conn, put_conn
from utils.logger import get_logger

logger = get_logger(__name__)

# ── DDL statements in dependency order ───────────────────────────────────────
_DDL: list[str] = [

    # ── Extensions ────────────────────────────────────────────────────────── #
    'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"',

    # ── Dimension: dim_date ───────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key     INTEGER  PRIMARY KEY,
        full_date    DATE     NOT NULL,
        year         SMALLINT NOT NULL,
        quarter      SMALLINT NOT NULL,
        month        SMALLINT NOT NULL,
        month_name   VARCHAR(10) NOT NULL,
        week         SMALLINT NOT NULL,
        day_of_week  SMALLINT NOT NULL,
        day_of_month SMALLINT NOT NULL,
        day_of_year  SMALLINT NOT NULL,
        day_name     VARCHAR(10) NOT NULL
    )
    """,

    # ── Dimension: dim_time ───────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_time (
        time_key INTEGER  PRIMARY KEY,
        full_time TIME    NOT NULL,
        hour      SMALLINT NOT NULL,
        minute    SMALLINT NOT NULL,
        am_pm     CHAR(2)  NOT NULL,
        shift     VARCHAR(10) NOT NULL
    )
    """,

    # ── Dimension: dim_region ─────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_region (
        region_id         INTEGER      PRIMARY KEY,
        region_name       VARCHAR(100) NOT NULL,
        city_name         VARCHAR(100),
        delivery_base_fee NUMERIC(8,2)
    )
    """,

    # ── Dimension: dim_segment ────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_segment (
        segment_id       INTEGER      PRIMARY KEY,
        segment_name     VARCHAR(50)  NOT NULL,
        discount_pct     NUMERIC(5,2),
        priority_support BOOLEAN
    )
    """,

    # ── Dimension: dim_category ───────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_category (
        category_id   INTEGER      PRIMARY KEY,
        category_name VARCHAR(100) NOT NULL
    )
    """,

    # ── Dimension: dim_team ───────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_team (
        team_id   INTEGER     PRIMARY KEY,
        team_name VARCHAR(100)
    )
    """,

    # ── Dimension: dim_reason_category ───────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_reason_category (
        reason_category_id INTEGER     PRIMARY KEY,
        category_name      VARCHAR(100) NOT NULL
    )
    """,

    # ── Dimension: dim_reason ─────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_reason (
        reason_id          INTEGER      PRIMARY KEY,
        reason_name        VARCHAR(200),
        reason_category_id INTEGER      REFERENCES dim_reason_category(reason_category_id),
        reason_category    VARCHAR(100),
        severity_level     SMALLINT,
        typical_refund_pct NUMERIC(4,2)
    )
    """,

    # ── Dimension: dim_channel ────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_channel (
        channel_id   INTEGER     PRIMARY KEY,
        channel_name VARCHAR(50) NOT NULL
    )
    """,

    # ── Dimension: dim_priority ───────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_priority (
        priority_id            INTEGER     PRIMARY KEY,
        priority_code          VARCHAR(5)  NOT NULL,
        priority_name          VARCHAR(50) NOT NULL,
        sla_first_response_min SMALLINT,
        sla_resolution_min     SMALLINT
    )
    """,

    # ── Dimension: dim_customer ───────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id  INTEGER      PRIMARY KEY,
        full_name    VARCHAR(200),
        email        VARCHAR(64),
        phone        VARCHAR(64),
        region_id    INTEGER      REFERENCES dim_region(region_id),
        segment_id   INTEGER      REFERENCES dim_segment(segment_id),
        signup_date  DATE,
        gender       VARCHAR(10),
        created_at   TIMESTAMP,
        updated_at   TIMESTAMP,
        _loaded_at   TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,

    # ── Dimension: dim_restaurant ─────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_restaurant (
        restaurant_id     INTEGER      PRIMARY KEY,
        restaurant_name   VARCHAR(200),
        region_id         INTEGER      REFERENCES dim_region(region_id),
        category_id       INTEGER      REFERENCES dim_category(category_id),
        price_tier        VARCHAR(10),
        rating_avg        NUMERIC(4,2),
        prep_time_avg_min SMALLINT,
        is_active         BOOLEAN,
        created_at        TIMESTAMP,
        updated_at        TIMESTAMP,
        _loaded_at        TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,

    # ── Dimension: dim_driver ─────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_driver (
        driver_id            INTEGER      PRIMARY KEY,
        driver_name          VARCHAR(200),
        driver_phone         VARCHAR(64),
        national_id          VARCHAR(64),
        region_id            INTEGER      REFERENCES dim_region(region_id),
        shift                VARCHAR(10),
        vehicle_type         VARCHAR(20),
        hire_date            DATE,
        rating_avg           NUMERIC(4,2),
        on_time_rate         NUMERIC(5,3),
        cancel_rate          NUMERIC(5,3),
        completed_deliveries INTEGER,
        is_active            BOOLEAN,
        created_at           TIMESTAMP,
        updated_at           TIMESTAMP,
        _loaded_at           TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,

    # ── Dimension: dim_agent ──────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS dim_agent (
        agent_id            INTEGER      PRIMARY KEY,
        agent_name          VARCHAR(200),
        agent_email         VARCHAR(64),
        agent_phone         VARCHAR(64),
        team_id             INTEGER      REFERENCES dim_team(team_id),
        skill_level         VARCHAR(10),
        hire_date           DATE,
        avg_handle_time_min SMALLINT,
        resolution_rate     NUMERIC(5,3),
        csat_score          NUMERIC(4,2),
        is_active           BOOLEAN,
        created_at          TIMESTAMP,
        updated_at          TIMESTAMP,
        _loaded_at          TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,

    # ── Fact: fact_order ──────────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS fact_order (
        order_sk        BIGSERIAL    PRIMARY KEY,
        order_id        VARCHAR(36)  NOT NULL UNIQUE,
        customer_id     INTEGER      REFERENCES dim_customer(customer_id),
        restaurant_id   INTEGER      REFERENCES dim_restaurant(restaurant_id),
        driver_id       INTEGER      REFERENCES dim_driver(driver_id),
        region_id       INTEGER      REFERENCES dim_region(region_id),
        date_key        INTEGER      REFERENCES dim_date(date_key),
        time_key        INTEGER      REFERENCES dim_time(time_key),
        order_status    VARCHAR(20),
        payment_method  VARCHAR(20),
        created_at      TIMESTAMP,
        delivered_at    TIMESTAMP,
        order_amount    NUMERIC(10,2),
        delivery_fee    NUMERIC(8,2),
        discount_amount NUMERIC(8,2),
        total_amount    NUMERIC(10,2),
        _loaded_at      TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,

    # ── Fact: fact_ticket ─────────────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS fact_ticket (
        ticket_sk               BIGSERIAL   PRIMARY KEY,
        ticket_id               VARCHAR(36) NOT NULL UNIQUE,
        order_id                VARCHAR(36) REFERENCES fact_order(order_id),
        customer_id             INTEGER     REFERENCES dim_customer(customer_id),
        region_id               INTEGER     REFERENCES dim_region(region_id),
        restaurant_id           INTEGER     REFERENCES dim_restaurant(restaurant_id),
        driver_id               INTEGER     REFERENCES dim_driver(driver_id),
        agent_id                INTEGER     REFERENCES dim_agent(agent_id),
        reason_id               INTEGER     REFERENCES dim_reason(reason_id),
        priority_id             INTEGER     REFERENCES dim_priority(priority_id),
        channel_id              INTEGER     REFERENCES dim_channel(channel_id),
        date_key                INTEGER     REFERENCES dim_date(date_key),
        time_key                INTEGER     REFERENCES dim_time(time_key),
        status                  VARCHAR(20),
        refund_amount           NUMERIC(10,2),
        created_at              TIMESTAMP,
        first_response_at       TIMESTAMP,
        resolved_at             TIMESTAMP,
        sla_first_due_at        TIMESTAMP,
        sla_resolve_due_at      TIMESTAMP,
        first_response_min      NUMERIC(8,2),
        resolution_min          NUMERIC(8,2),
        sla_response_breached   BOOLEAN DEFAULT FALSE,
        sla_resolution_breached BOOLEAN DEFAULT FALSE,
        reopened                BOOLEAN DEFAULT FALSE,
        _loaded_at              TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,

    # ── Fact: fact_ticket_event ───────────────────────────────────────────── #
    """
    CREATE TABLE IF NOT EXISTS fact_ticket_event (
        event_sk   BIGSERIAL   PRIMARY KEY,
        event_id   VARCHAR(36) NOT NULL UNIQUE,
        ticket_id  VARCHAR(36) REFERENCES fact_ticket(ticket_id),
        agent_id   INTEGER     REFERENCES dim_agent(agent_id),
        date_key   INTEGER     REFERENCES dim_date(date_key),
        time_key   INTEGER     REFERENCES dim_time(time_key),
        event_ts   TIMESTAMP,
        old_status VARCHAR(20),
        new_status VARCHAR(20) NOT NULL,
        notes      TEXT,
        _loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,

    # ── Quarantine ────────────────────────────────────────────────────────── #
    # Stores rejected records from any table as JSONB so one table works for all.
    # source_table tells you which dim/fact it came from.
    # rejection_stage: 'schema' | 'records' | 'orphan'
    # You will write a stored procedure / query on this table to promote
    # true orphans after 24 hours into the true_orphan table.
    """
    CREATE TABLE IF NOT EXISTS quarantine (
        quarantine_id    BIGSERIAL    PRIMARY KEY,
        source_file      TEXT         NOT NULL,
        source_table     TEXT,
        rejection_reason TEXT         NOT NULL,
        rejection_stage  VARCHAR(20)  NOT NULL,
        raw_data         JSONB        NOT NULL,
        quarantined_at   TIMESTAMP    NOT NULL DEFAULT NOW()
    )
    """,

    # ── True orphan ───────────────────────────────────────────────────────── #
    # Populated by your stored procedure after 24h retry window.
    """
    CREATE TABLE IF NOT EXISTS true_orphan (
        orphan_id      BIGSERIAL   PRIMARY KEY,
        quarantine_id  BIGINT,
        source_file    TEXT        NOT NULL,
        source_table   TEXT,
        unresolved_fk  VARCHAR(50) NOT NULL,
        raw_data       JSONB       NOT NULL,
        first_seen_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
        retry_count    SMALLINT    NOT NULL DEFAULT 0,
        last_retry_at  TIMESTAMP
    )
    """,

    # ── Pipeline file log ─────────────────────────────────────────────────── #
    # Analytics-facing record of every file handled by the pipeline.
    # Replaces SQLite tracker for visibility — SQLite tracker still runs for
    # idempotency (it is faster for that purpose).
    """
    CREATE TABLE IF NOT EXISTS pipeline_file_log (
        log_id          BIGSERIAL   PRIMARY KEY,
        file_path       TEXT        NOT NULL,
        file_name       TEXT        NOT NULL,
        file_hash       VARCHAR(32) NOT NULL,
        file_type       VARCHAR(10) NOT NULL,
        source_table    TEXT,
        status          VARCHAR(10) NOT NULL,
        rejection_stage VARCHAR(20),
        total_records   INTEGER     DEFAULT 0,
        valid_records   INTEGER     DEFAULT 0,
        quarantined     INTEGER     DEFAULT 0,
        duplicate_count INTEGER     DEFAULT 0,
        orphan_count    INTEGER     DEFAULT 0,
        processed_at    TIMESTAMP   NOT NULL DEFAULT NOW()
    )
    """,

    # ── Indexes ───────────────────────────────────────────────────────────── #
    "CREATE INDEX IF NOT EXISTS idx_fact_order_customer   ON fact_order(customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_restaurant ON fact_order(restaurant_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_driver     ON fact_order(driver_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_region     ON fact_order(region_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_date       ON fact_order(date_key)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_status     ON fact_order(order_status)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_created    ON fact_order(created_at)",

    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_order     ON fact_ticket(order_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_customer  ON fact_ticket(customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_agent     ON fact_ticket(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_priority  ON fact_ticket(priority_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_channel   ON fact_ticket(channel_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_reason    ON fact_ticket(reason_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_date      ON fact_ticket(date_key)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_region    ON fact_ticket(region_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_status    ON fact_ticket(status)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_sla_r     ON fact_ticket(sla_response_breached)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_sla_res   ON fact_ticket(sla_resolution_breached)",

    "CREATE INDEX IF NOT EXISTS idx_ticket_event_ticket   ON fact_ticket_event(ticket_id)",
    "CREATE INDEX IF NOT EXISTS idx_ticket_event_ts       ON fact_ticket_event(event_ts)",

    "CREATE INDEX IF NOT EXISTS idx_quarantine_reason     ON quarantine(rejection_reason)",
    "CREATE INDEX IF NOT EXISTS idx_quarantine_stage      ON quarantine(rejection_stage)",
    "CREATE INDEX IF NOT EXISTS idx_quarantine_table      ON quarantine(source_table)",
    "CREATE INDEX IF NOT EXISTS idx_quarantine_at         ON quarantine(quarantined_at)",

    "CREATE INDEX IF NOT EXISTS idx_true_orphan_fk        ON true_orphan(unresolved_fk)",
    "CREATE INDEX IF NOT EXISTS idx_true_orphan_seen      ON true_orphan(first_seen_at)",

    "CREATE INDEX IF NOT EXISTS idx_file_log_name         ON pipeline_file_log(file_name)",
    "CREATE INDEX IF NOT EXISTS idx_file_log_status       ON pipeline_file_log(status)",
    "CREATE INDEX IF NOT EXISTS idx_file_log_processed    ON pipeline_file_log(processed_at)",
]


def init_schema() -> None:
    """
    Execute all DDL statements.
    Called once from ingestion_runner.start() after init_pool().
    Safe to call on every run — all statements use IF NOT EXISTS.
    """
    conn = get_conn()
    try:
        with conn:
            cur = conn.cursor()
            for stmt in _DDL:
                try:
                    cur.execute(stmt)
                except Exception as exc:
                    logger.error(f"[schema] DDL error: {exc} | stmt: {stmt[:60]}...")
            cur.close()
        logger.info("[schema] All tables verified / created in fastfeastapp.")
    finally:
        put_conn(conn)
