-- ============================================================
-- FastFeast Data Warehouse — PostgreSQL DDL
-- Database: fastfeastapp
-- ============================================================
-- Design principles:
--   • No CHECK constraints on business columns — validation layer owns that
--   • FK REFERENCES enforced on all fact and dimension tables for
--     correct JOIN behaviour and query planner optimisation
--   • orphan_validator ensures no row reaches a fact table with a
--     missing parent — FK constraints are the final safety net
--   • Quarantine stores raw_data as JSONB — works for any table shape
--   • pipeline_file_log is the analytics-facing file tracking record
--
-- Run on a fresh database:
--   psql -U postgres -d fastfeastapp -f schema_ddl.sql
--
-- The pipeline creates the database automatically on startup via
-- datawarehouse/db_connection.py _create_database_if_not_exists().
-- This file is provided for manual inspection / re-creation only.
-- ============================================================


-- ============================================================
-- EXTENSIONS
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ============================================================
-- DROP ORDER (reverse dependency)
-- ============================================================
DROP TABLE IF EXISTS pipeline_file_log    CASCADE;
DROP TABLE IF EXISTS true_orphan          CASCADE;
DROP TABLE IF EXISTS quarantine           CASCADE;
DROP TABLE IF EXISTS fact_ticket_event    CASCADE;
DROP TABLE IF EXISTS fact_ticket          CASCADE;
DROP TABLE IF EXISTS fact_order           CASCADE;
DROP TABLE IF EXISTS dim_agent            CASCADE;
DROP TABLE IF EXISTS dim_customer         CASCADE;
DROP TABLE IF EXISTS dim_driver           CASCADE;
DROP TABLE IF EXISTS dim_restaurant       CASCADE;
DROP TABLE IF EXISTS dim_reason           CASCADE;
DROP TABLE IF EXISTS dim_reason_category  CASCADE;
DROP TABLE IF EXISTS dim_priority         CASCADE;
DROP TABLE IF EXISTS dim_channel          CASCADE;
DROP TABLE IF EXISTS dim_segment          CASCADE;
DROP TABLE IF EXISTS dim_category         CASCADE;
DROP TABLE IF EXISTS dim_team             CASCADE;
DROP TABLE IF EXISTS dim_region           CASCADE;
DROP TABLE IF EXISTS dim_time             CASCADE;
DROP TABLE IF EXISTS dim_date             CASCADE;


-- ============================================================
-- DIMENSION: dim_date
-- ============================================================
CREATE TABLE dim_date (
    date_key     INTEGER     PRIMARY KEY,    -- YYYYMMDD
    full_date    DATE        NOT NULL,
    year         SMALLINT    NOT NULL,
    quarter      SMALLINT    NOT NULL,
    month        SMALLINT    NOT NULL,
    month_name   VARCHAR(10) NOT NULL,
    week         SMALLINT    NOT NULL,
    day_of_week  SMALLINT    NOT NULL,
    day_of_month SMALLINT    NOT NULL,
    day_of_year  SMALLINT    NOT NULL,
    day_name     VARCHAR(10) NOT NULL
);


-- ============================================================
-- DIMENSION: dim_time
-- ============================================================
CREATE TABLE dim_time (
    time_key  INTEGER     PRIMARY KEY,    -- HHMM
    full_time TIME        NOT NULL,
    hour      SMALLINT    NOT NULL,
    minute    SMALLINT    NOT NULL,
    am_pm     CHAR(2)     NOT NULL,
    shift     VARCHAR(10) NOT NULL        -- 'morning' | 'evening' | 'night'
);


-- ============================================================
-- DIMENSION: dim_region
-- city_name denormalised from cities.json
-- ============================================================
CREATE TABLE dim_region (
    region_id         INTEGER      PRIMARY KEY,
    region_name       VARCHAR(100) NOT NULL,
    city_name         VARCHAR(100),
    delivery_base_fee NUMERIC(8,2)
);


-- ============================================================
-- DIMENSION: dim_segment
-- ============================================================
CREATE TABLE dim_segment (
    segment_id       INTEGER     PRIMARY KEY,
    segment_name     VARCHAR(50) NOT NULL,
    discount_pct     NUMERIC(5,2),
    priority_support BOOLEAN
);


-- ============================================================
-- DIMENSION: dim_category
-- ============================================================
CREATE TABLE dim_category (
    category_id   INTEGER      PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);


-- ============================================================
-- DIMENSION: dim_team
-- ============================================================
CREATE TABLE dim_team (
    team_id   INTEGER      PRIMARY KEY,
    team_name VARCHAR(100)
);


-- ============================================================
-- DIMENSION: dim_reason_category
-- ============================================================
CREATE TABLE dim_reason_category (
    reason_category_id INTEGER      PRIMARY KEY,
    category_name      VARCHAR(100) NOT NULL
);


-- ============================================================
-- DIMENSION: dim_reason
-- reason_category denormalised from dim_reason_category
-- ============================================================
CREATE TABLE dim_reason (
    reason_id          INTEGER      PRIMARY KEY,
    reason_name        VARCHAR(200),
    reason_category_id INTEGER      REFERENCES dim_reason_category(reason_category_id),
    reason_category    VARCHAR(100),
    severity_level     SMALLINT,
    typical_refund_pct NUMERIC(4,2)
);


-- ============================================================
-- DIMENSION: dim_channel
-- ============================================================
CREATE TABLE dim_channel (
    channel_id   INTEGER     PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL
);


-- ============================================================
-- DIMENSION: dim_priority
-- ============================================================
CREATE TABLE dim_priority (
    priority_id            INTEGER    PRIMARY KEY,
    priority_code          VARCHAR(5) NOT NULL,
    priority_name          VARCHAR(50) NOT NULL,
    sla_first_response_min SMALLINT,
    sla_resolution_min     SMALLINT
);


-- ============================================================
-- DIMENSION: dim_customer
-- PII columns (email, phone) are SHA-256 hashed by pii_handler
-- before the row reaches this table.
-- ============================================================
CREATE TABLE dim_customer (
    customer_id  INTEGER      PRIMARY KEY,
    full_name    VARCHAR(200),
    email        VARCHAR(64),            -- SHA-256 hash
    phone        VARCHAR(64),            -- SHA-256 hash
    region_id    INTEGER      REFERENCES dim_region(region_id),
    segment_id   INTEGER      REFERENCES dim_segment(segment_id),
    signup_date  DATE,
    gender       VARCHAR(10),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    _loaded_at   TIMESTAMP NOT NULL DEFAULT NOW()
);


-- ============================================================
-- DIMENSION: dim_driver
-- PII columns (driver_phone, national_id) are SHA-256 hashed.
-- ============================================================
CREATE TABLE dim_driver (
    driver_id            INTEGER      PRIMARY KEY,
    driver_name          VARCHAR(200),
    driver_phone         VARCHAR(64),    -- SHA-256 hash
    national_id          VARCHAR(64),    -- SHA-256 hash
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
);


-- ============================================================
-- DIMENSION: dim_restaurant
-- ============================================================
CREATE TABLE dim_restaurant (
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
);


-- ============================================================
-- DIMENSION: dim_agent
-- PII columns (agent_email, agent_phone) are SHA-256 hashed.
-- ============================================================
CREATE TABLE dim_agent (
    agent_id            INTEGER      PRIMARY KEY,
    agent_name          VARCHAR(200),
    agent_email         VARCHAR(64),    -- SHA-256 hash
    agent_phone         VARCHAR(64),    -- SHA-256 hash
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
);


-- ============================================================
-- FACT: fact_order
-- Grain: one row per order
-- orphan_validator guarantees all FK values exist before INSERT.
-- FK REFERENCES enforced here for JOIN correctness and query planner.
-- ============================================================
CREATE TABLE fact_order (
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
);


-- ============================================================
-- FACT: fact_ticket
-- Grain: one row per support ticket
-- SLA breach flags and response/resolution minutes calculated
-- by fact_loader.py before INSERT.
-- FK REFERENCES enforced for JOIN correctness and query planner.
-- ============================================================
CREATE TABLE fact_ticket (
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
);


-- ============================================================
-- FACT: fact_ticket_event
-- Grain: one row per status-change event on a ticket
-- FK REFERENCES enforced for JOIN correctness and query planner.
-- ============================================================
CREATE TABLE fact_ticket_event (
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
);


-- ============================================================
-- QUARANTINE
-- One table for all rejected records from any source.
-- raw_data JSONB — works for any table shape without schema changes.
--
-- rejection_stage values:
--   'schema'  — failed schema validation (wrong columns/types/nulls)
--   'records' — failed business rules (format, range, duplicate)
--   'orphan'  — failed referential integrity (FK not found in DWH)
--
-- Your stored procedure for true orphan detection queries this table.
-- See: stored_procedure_true_orphans.sql
-- ============================================================
CREATE TABLE quarantine (
    quarantine_id    BIGSERIAL   PRIMARY KEY,
    source_file      TEXT        NOT NULL,
    source_table     TEXT,
    rejection_reason TEXT        NOT NULL,
    rejection_stage  VARCHAR(20) NOT NULL,   -- 'schema' | 'records' | 'orphan'
    raw_data         JSONB       NOT NULL,
    quarantined_at   TIMESTAMP   NOT NULL DEFAULT NOW()
);


-- ============================================================
-- TRUE ORPHAN
-- Populated by stored procedure after 24-hour retry window.
-- Records here have been confirmed as unresolvable — the
-- referenced dimension record never arrived in any batch.
-- ============================================================
CREATE TABLE true_orphan (
    orphan_id     BIGSERIAL   PRIMARY KEY,
    quarantine_id BIGINT,
    source_file   TEXT        NOT NULL,
    source_table  TEXT,
    unresolved_fk VARCHAR(50) NOT NULL,
    raw_data      JSONB       NOT NULL,
    first_seen_at TIMESTAMP   NOT NULL DEFAULT NOW(),
    retry_count   SMALLINT    NOT NULL DEFAULT 0,
    last_retry_at TIMESTAMP
);


-- ============================================================
-- PIPELINE FILE LOG
-- Analytics-facing record of every file processed.
-- One row per file per pipeline run.
-- Complements SQLite file_tracker (which handles idempotency).
--
-- status          : 'success' | 'failed'
-- rejection_stage : 'schema' | 'records' | 'orphan' | 'load' | NULL (success)
-- ============================================================
CREATE TABLE pipeline_file_log (
    log_id          BIGSERIAL   PRIMARY KEY,
    file_path       TEXT        NOT NULL,
    file_name       TEXT        NOT NULL,
    file_hash       VARCHAR(32) NOT NULL,
    file_type       VARCHAR(10) NOT NULL,    -- 'batch' | 'stream'
    source_table    TEXT,
    status          VARCHAR(10) NOT NULL,    -- 'success' | 'failed'
    rejection_stage VARCHAR(20),
    total_records   INTEGER     DEFAULT 0,
    valid_records   INTEGER     DEFAULT 0,
    quarantined     INTEGER     DEFAULT 0,
    duplicate_count INTEGER     DEFAULT 0,
    orphan_count    INTEGER     DEFAULT 0,
    processed_at    TIMESTAMP   NOT NULL DEFAULT NOW()
);


-- ============================================================
-- INDEXES
-- ============================================================

-- fact_order
CREATE INDEX idx_fact_order_customer    ON fact_order(customer_id);
CREATE INDEX idx_fact_order_restaurant  ON fact_order(restaurant_id);
CREATE INDEX idx_fact_order_driver      ON fact_order(driver_id);
CREATE INDEX idx_fact_order_region      ON fact_order(region_id);
CREATE INDEX idx_fact_order_date        ON fact_order(date_key);
CREATE INDEX idx_fact_order_status      ON fact_order(order_status);
CREATE INDEX idx_fact_order_created     ON fact_order(created_at);

-- fact_ticket
CREATE INDEX idx_fact_ticket_order      ON fact_ticket(order_id);
CREATE INDEX idx_fact_ticket_customer   ON fact_ticket(customer_id);
CREATE INDEX idx_fact_ticket_agent      ON fact_ticket(agent_id);
CREATE INDEX idx_fact_ticket_priority   ON fact_ticket(priority_id);
CREATE INDEX idx_fact_ticket_channel    ON fact_ticket(channel_id);
CREATE INDEX idx_fact_ticket_reason     ON fact_ticket(reason_id);
CREATE INDEX idx_fact_ticket_date       ON fact_ticket(date_key);
CREATE INDEX idx_fact_ticket_region     ON fact_ticket(region_id);
CREATE INDEX idx_fact_ticket_status     ON fact_ticket(status);
CREATE INDEX idx_fact_ticket_sla_r      ON fact_ticket(sla_response_breached);
CREATE INDEX idx_fact_ticket_sla_res    ON fact_ticket(sla_resolution_breached);

-- fact_ticket_event
CREATE INDEX idx_ticket_event_ticket    ON fact_ticket_event(ticket_id);
CREATE INDEX idx_ticket_event_ts        ON fact_ticket_event(event_ts);

-- quarantine
CREATE INDEX idx_quarantine_reason      ON quarantine(rejection_reason);
CREATE INDEX idx_quarantine_stage       ON quarantine(rejection_stage);
CREATE INDEX idx_quarantine_table       ON quarantine(source_table);
CREATE INDEX idx_quarantine_at          ON quarantine(quarantined_at);

-- true_orphan
CREATE INDEX idx_true_orphan_fk         ON true_orphan(unresolved_fk);
CREATE INDEX idx_true_orphan_seen       ON true_orphan(first_seen_at);

-- pipeline_file_log
CREATE INDEX idx_file_log_name          ON pipeline_file_log(file_name);
CREATE INDEX idx_file_log_status        ON pipeline_file_log(status);
CREATE INDEX idx_file_log_processed     ON pipeline_file_log(processed_at);


-- ============================================================
-- HELPERS: Populate dim_date (2019-01-01 → 2030-12-31)
-- ============================================================
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    week, day_of_week, day_of_month, day_of_year, day_name
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER      AS date_key,
    d::DATE                               AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT     AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT     AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT     AS month,
    TO_CHAR(d, 'Month')                   AS month_name,
    EXTRACT(WEEK    FROM d)::SMALLINT     AS week,
    EXTRACT(DOW     FROM d)::SMALLINT + 1 AS day_of_week,
    EXTRACT(DAY     FROM d)::SMALLINT     AS day_of_month,
    EXTRACT(DOY     FROM d)::SMALLINT     AS day_of_year,
    TO_CHAR(d, 'Day')                     AS day_name
FROM generate_series(
    '2019-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS t(d);


-- ============================================================
-- HELPERS: Populate dim_time (all 1440 minutes)
-- ============================================================
INSERT INTO dim_time (time_key, full_time, hour, minute, am_pm, shift)
SELECT
    (h * 100 + m)                             AS time_key,
    MAKE_TIME(h, m, 0)                        AS full_time,
    h::SMALLINT                               AS hour,
    m::SMALLINT                               AS minute,
    CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END  AS am_pm,
    CASE
        WHEN h BETWEEN 6  AND 13 THEN 'morning'
        WHEN h BETWEEN 14 AND 21 THEN 'evening'
        ELSE                          'night'
    END                                       AS shift
FROM
    generate_series(0, 23) AS h,
    generate_series(0, 59) AS m;
