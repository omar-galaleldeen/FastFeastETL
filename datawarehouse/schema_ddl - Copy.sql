-- ============================================================
-- FastFeast Data Warehouse — PostgreSQL DDL
-- Star Schema
-- ============================================================
-- Execution order matters: parent tables before children.
-- Run once on a fresh database:
--   psql -U postgres -d fastfeast -f schema_ddl.sql
-- ============================================================


-- ============================================================
-- EXTENSIONS
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ============================================================
-- DROP (for clean re-runs during development)
-- ============================================================
DROP TABLE IF EXISTS pipeline_quality_log  CASCADE;
DROP TABLE IF EXISTS true_orphan           CASCADE;
DROP TABLE IF EXISTS quarantine            CASCADE;
DROP TABLE IF EXISTS fact_ticket_event     CASCADE;
DROP TABLE IF EXISTS fact_ticket           CASCADE;
DROP TABLE IF EXISTS fact_order            CASCADE;
DROP TABLE IF EXISTS dim_agent             CASCADE;
DROP TABLE IF EXISTS dim_customer          CASCADE;
DROP TABLE IF EXISTS dim_driver            CASCADE;
DROP TABLE IF EXISTS dim_restaurant        CASCADE;
DROP TABLE IF EXISTS dim_reason            CASCADE;
DROP TABLE IF EXISTS dim_priority          CASCADE;
DROP TABLE IF EXISTS dim_channel           CASCADE;
DROP TABLE IF EXISTS dim_region            CASCADE;
DROP TABLE IF EXISTS dim_time              CASCADE;
DROP TABLE IF EXISTS dim_date              CASCADE;


-- ============================================================
-- DIMENSION: dim_date
-- Populated once via a date-generation procedure.
-- ============================================================
CREATE TABLE dim_date (
    date_key        INTEGER         PRIMARY KEY,        -- YYYYMMDD
    full_date       DATE            NOT NULL UNIQUE,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month           SMALLINT        NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name      VARCHAR(10)     NOT NULL,
    week            SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_of_month    SMALLINT        NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_year     SMALLINT        NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),
    day_name        VARCHAR(10)     NOT NULL
);


-- ============================================================
-- DIMENSION: dim_time
-- Populated once with all 1440 minutes of the day.
-- ============================================================
CREATE TABLE dim_time (
    time_key        INTEGER         PRIMARY KEY,        -- HHMM integer key
    full_time       TIME            NOT NULL,
    hour            SMALLINT        NOT NULL CHECK (hour BETWEEN 0 AND 23),
    minute          SMALLINT        NOT NULL CHECK (minute BETWEEN 0 AND 59),
    am_pm           CHAR(2)         NOT NULL CHECK (am_pm IN ('AM', 'PM')),
    shift           VARCHAR(10)     NOT NULL            -- 'morning' | 'evening' | 'night'
);


-- ============================================================
-- DIMENSION: dim_region
-- Source: regions.csv
-- Columns: region_id, region_name, city_id, delivery_base_fee
--   + city_name denormalised from cities.csv (per data model PDF)
-- ============================================================
CREATE TABLE dim_region (
    region_id           INTEGER         PRIMARY KEY,
    region_name         VARCHAR(100)    NOT NULL,
    city_name           VARCHAR(100)    NOT NULL,           -- denormalised from cities
    delivery_base_fee   NUMERIC(8, 2)   NOT NULL CHECK (delivery_base_fee >= 0)
);


-- ============================================================
-- DIMENSION: dim_customer
-- Source: customers.csv
-- PII: email and phone are hashed by the pipeline before load.
-- ============================================================
CREATE TABLE dim_customer (
    customer_id     INTEGER         PRIMARY KEY,
    customer_name   VARCHAR(200)    NOT NULL,
    region_id       INTEGER         REFERENCES dim_region(region_id),
    segment         VARCHAR(50),                            -- 'Regular' | 'VIP'
    gender          VARCHAR(10),                            -- 'male' | 'female'
    signup_date_id  INTEGER         REFERENCES dim_date(date_key),
    --email_hash      VARCHAR(64),                            -- SHA-256, PII masked  -- not used in current application
    --phone_hash      VARCHAR(64),                            -- SHA-256, PII masked  -- not used in current application
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);


-- ============================================================
-- DIMENSION: dim_driver
-- Source: drivers.csv
-- PII: phone and national_id hashed by pipeline before load.
-- ============================================================
CREATE TABLE dim_driver (
    driver_id               INTEGER         PRIMARY KEY,
    driver_name             VARCHAR(200)    NOT NULL,
    --phone_hash              VARCHAR(64),                    -- SHA-256, PII masked -- not used in current application 
    --national_id_hash        VARCHAR(64),                    -- SHA-256, PII masked -- not used in current application
    region_id               INTEGER         REFERENCES dim_region(region_id),
    shift                   VARCHAR(10)     CHECK (shift IN ('morning', 'evening', 'night')),
    vehicle_type            VARCHAR(20)     CHECK (vehicle_type IN ('bike', 'motorbike', 'car')),
    hire_date               DATE,
    rating_avg              NUMERIC(3, 2)   CHECK (rating_avg BETWEEN 1.0 AND 5.0),
    on_time_rate            NUMERIC(5, 3)   CHECK (on_time_rate BETWEEN 0.0 AND 1.0),
    cancel_rate             NUMERIC(5, 3)   CHECK (cancel_rate BETWEEN 0.0 AND 1.0),
    completed_deliveries    INTEGER         DEFAULT 0,
    status                  VARCHAR(10)     NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active', 'inactive')),
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
);


-- ============================================================
-- DIMENSION: dim_restaurant
-- Source: restaurants.json
-- ============================================================
CREATE TABLE dim_restaurant (
    restaurant_id       INTEGER         PRIMARY KEY,
    restaurant_name     VARCHAR(200)    NOT NULL,
    category_name       VARCHAR(100),                       -- denormalised from categories
    region_id           INTEGER         REFERENCES dim_region(region_id),
    price_tier          VARCHAR(10)     CHECK (price_tier IN ('Low', 'Mid', 'High')),
    rating_avg          NUMERIC(3, 2)   CHECK (rating_avg BETWEEN 1.0 AND 5.0),
    prep_time_avg_min   SMALLINT        CHECK (prep_time_avg_min > 0),
    status              VARCHAR(10)     NOT NULL DEFAULT 'active'
                                        CHECK (status IN ('active', 'inactive')),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);


-- ============================================================
-- DIMENSION: dim_agent
-- Source: agents.csv
-- PII: email hashed by pipeline before load.
-- ============================================================
CREATE TABLE dim_agent (
    agent_id                INTEGER         PRIMARY KEY,
    agent_name              VARCHAR(200)    NOT NULL,
    team_name               VARCHAR(100)   CHECK (team_name in ('General Support','Escalations','VIP Support','Technical','Refunds')),                -- denormalised from teams
    skill_level             VARCHAR(10)     CHECK (skill_level IN ('Junior', 'Mid', 'Senior', 'Lead')),
    hire_date_id            INTEGER         REFERENCES dim_date(date_key),
    avg_handle_time_min     SMALLINT        CHECK (avg_handle_time_min > 0),
    resolution_rate         NUMERIC(5, 3)   CHECK (resolution_rate BETWEEN 0.0 AND 1.0),
    csat_score              NUMERIC(3, 2)   CHECK (csat_score BETWEEN 1.0 AND 5.0),
    email_hash              VARCHAR(64),                    -- SHA-256, PII masked
    phone_hash              VARCHAR(64),                    -- SHA-256, PII masked
    status                  VARCHAR(10)     NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active', 'inactive')),
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
);


-- ============================================================
-- DIMENSION: dim_priority
-- Source: priorities.csv
-- ============================================================
CREATE TABLE dim_priority (
    priority_id             INTEGER         PRIMARY KEY,
    priority_code           VARCHAR(5)      NOT NULL UNIQUE,    -- P1, P2, P3, P4
    priority_name           VARCHAR(50)     NOT NULL,
    sla_first_response_min  SMALLINT        NOT NULL CHECK (sla_first_response_min > 0),
    sla_resolution_min      SMALLINT        NOT NULL CHECK (sla_resolution_min > 0)
);


-- ============================================================
-- DIMENSION: dim_channel
-- Source: channels.csv
-- ============================================================
CREATE TABLE dim_channel (
    channel_id      INTEGER         PRIMARY KEY,
    channel_name    VARCHAR(50)     NOT NULL UNIQUE      -- 'app' | 'chat' | 'phone' | 'email'
);


-- ============================================================
-- DIMENSION: dim_reason
-- Source: reasons.csv + reason_categories.csv (denormalised)
-- ============================================================
CREATE TABLE dim_reason (
    reason_id           INTEGER         PRIMARY KEY,
    reason_name         VARCHAR(200)    NOT NULL,
    reason_category     VARCHAR(100),                       -- denormalised from reason_categories
    severity_level      SMALLINT        CHECK (severity_level BETWEEN 1 AND 5),
    typical_refund_pct  NUMERIC(4, 2)   CHECK (typical_refund_pct BETWEEN 0.0 AND 1.0)
);


-- ============================================================
-- FACT: fact_order
-- Source: orders.json (stream)
-- Grain: one row per order
-- ============================================================
CREATE TABLE fact_order (
    order_sk            BIGSERIAL       PRIMARY KEY,        -- surrogate key
    order_id            UUID            NOT NULL UNIQUE,    -- business key from source
    customer_id         INTEGER         REFERENCES dim_customer(customer_id),
    restaurant_id       INTEGER         REFERENCES dim_restaurant(restaurant_id),
    driver_id           INTEGER         REFERENCES dim_driver(driver_id),
    region_id           INTEGER         REFERENCES dim_region(region_id),
    date_key            INTEGER         REFERENCES dim_date(date_key),
    time_key            INTEGER         REFERENCES dim_time(time_key),
    order_status        VARCHAR(20)     CHECK (order_status IN ('Delivered', 'Cancelled', 'Refunded')),
    payment_method      VARCHAR(20)     CHECK (payment_method IN ('card', 'cash', 'wallet')),
    created_at          TIMESTAMP       NOT NULL,
    delivered_at        TIMESTAMP,
    order_amount        NUMERIC(10, 2)  NOT NULL CHECK (order_amount >= 0),
    delivery_fee        NUMERIC(8, 2)   CHECK (delivery_fee >= 0),
    discount_amount     NUMERIC(8, 2)   DEFAULT 0 CHECK (discount_amount >= 0),
    total_amount        NUMERIC(10, 2)  CHECK (total_amount >= 0),
    _loaded_at          TIMESTAMP       NOT NULL DEFAULT NOW()
);


-- ============================================================
-- FACT: fact_ticket
-- Source: tickets.csv (stream)
-- Grain: one row per support ticket
-- SLA breach flags are CALCULATED in the pipeline / OLAP layer.
-- ============================================================
CREATE TABLE fact_ticket (
    ticket_sk               BIGSERIAL       PRIMARY KEY,    -- surrogate key
    ticket_id               UUID            NOT NULL UNIQUE,-- business key
    order_id                UUID            NOT NULL
                                            REFERENCES fact_order(order_id),
    customer_id             INTEGER         REFERENCES dim_customer(customer_id),
    region_id               INTEGER         REFERENCES dim_region(region_id),
    restaurant_id           INTEGER         REFERENCES dim_restaurant(restaurant_id),
    driver_id               INTEGER         REFERENCES dim_driver(driver_id),
    agent_id                INTEGER         REFERENCES dim_agent(agent_id),
    reason_id               INTEGER         REFERENCES dim_reason(reason_id),
    priority_id             INTEGER         NOT NULL
                                            REFERENCES dim_priority(priority_id),
    channel_id              INTEGER         REFERENCES dim_channel(channel_id),
    date_key                INTEGER         REFERENCES dim_date(date_key),
    time_key                INTEGER         REFERENCES dim_time(time_key),
    status                  VARCHAR(20)     CHECK (status IN ('Open','InProgress','Resolved','Closed','Reopened')),
    refund_amount           NUMERIC(10, 2)  DEFAULT 0 CHECK (refund_amount >= 0),
    created_at              TIMESTAMP       NOT NULL,
    first_response_at       TIMESTAMP,
    resolved_at             TIMESTAMP,
    sla_first_due_at        TIMESTAMP,
    sla_resolve_due_at      TIMESTAMP,
    -- ── SLA metrics calculated by pipeline ──────────────────
    first_response_min      NUMERIC(8, 2),  -- actual minutes to first response
    resolution_min          NUMERIC(8, 2),  -- actual minutes to resolution
    sla_response_breached   BOOLEAN         DEFAULT FALSE,
    sla_resolution_breached BOOLEAN         DEFAULT FALSE,
    reopened                BOOLEAN         DEFAULT FALSE,
    _loaded_at              TIMESTAMP       NOT NULL DEFAULT NOW()
);


-- ============================================================
-- FACT: fact_ticket_event
-- Source: ticket_events.json (stream)
-- Grain: one row per status-change event on a ticket
-- ============================================================
CREATE TABLE fact_ticket_event (
    event_sk        BIGSERIAL       PRIMARY KEY,            -- surrogate key
    event_id        UUID            NOT NULL UNIQUE,        -- business key
    ticket_id       UUID            NOT NULL
                                    REFERENCES fact_ticket(ticket_id),
    agent_id        INTEGER         REFERENCES dim_agent(agent_id),
    date_key        INTEGER         REFERENCES dim_date(date_key),
    time_key        INTEGER         REFERENCES dim_time(time_key),
    event_ts        TIMESTAMP       NOT NULL,
    old_status      VARCHAR(20),
    new_status      VARCHAR(20)     NOT NULL,
    notes           TEXT,
    _loaded_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);


-- ============================================================
-- PIPELINE QUALITY LOG
-- One row per file processed by the pipeline.
-- ============================================================
CREATE TABLE pipeline_quality_log (
    log_id                      BIGSERIAL       PRIMARY KEY,
    timestamp                   TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_file                 TEXT            NOT NULL,
    source_type                 VARCHAR(10)     NOT NULL
                                                CHECK (source_type IN ('batch', 'stream')),
    stream_date                 DATE,
    stream_hour                 SMALLINT        CHECK (stream_hour BETWEEN 0 AND 23),   -- NULL for batch
    total_records               INTEGER         DEFAULT 0,
    null_stats                  JSONB,          -- per-column null counts  {"col": count, ...}
    duplicate_count             INTEGER         DEFAULT 0,
    duplicate_rate_pct          NUMERIC(6, 4),
    orphan_count                INTEGER         DEFAULT 0,
    orphan_rate_pct             NUMERIC(6, 4),
    referential_integrity_pct   NUMERIC(6, 4),
    sla_consistency_rate_pct    NUMERIC(6, 4),
    quarantine_count            INTEGER         DEFAULT 0,
    file_success                BOOLEAN         NOT NULL DEFAULT TRUE,
    latency_ms                  INTEGER,
    logged_at                   TIMESTAMP       NOT NULL DEFAULT NOW()
);


-- ============================================================
-- QUARANTINE
-- Records that failed validation but are kept for review.
-- Mirrors fact_order structure + rejection metadata.
-- ============================================================
CREATE TABLE quarantine (
    quarantine_id       BIGSERIAL       PRIMARY KEY,
    -- Original order fields (nullable — record may be partially valid)
    order_id            TEXT,
    customer_id         TEXT,
    restaurant_id       TEXT,
    driver_id           TEXT,
    region_id           TEXT,
    order_amount        TEXT,
    delivery_fee        TEXT,
    discount_amount     TEXT,
    total_amount        TEXT,
    order_status        TEXT,
    payment_method      TEXT,
    created_at          TEXT,
    delivered_at        TEXT,
    -- Rejection metadata
    source_file         TEXT            NOT NULL,
    rejection_reason    VARCHAR(100)    NOT NULL,   -- 'invalid_email' | 'orphan_customer_id' | ...
    quarantined_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);


-- ============================================================
-- TRUE ORPHANS
-- Orders where FK references could not be resolved even after
-- the next batch load.  Separated from general quarantine
-- for targeted investigation.
-- ============================================================
CREATE TABLE true_orphan (
    orphan_id           BIGSERIAL       PRIMARY KEY,
    order_id            TEXT,
    customer_id         TEXT,
    restaurant_id       TEXT,
    driver_id           TEXT,
    region_id           TEXT,
    order_amount        TEXT,
    delivery_fee        TEXT,
    discount_amount     TEXT,
    total_amount        TEXT,
    order_status        TEXT,
    payment_method      TEXT,
    created_at          TEXT,
    delivered_at        TEXT,
    -- Orphan metadata
    unresolved_fk       VARCHAR(50)     NOT NULL,   -- which FK failed: 'customer_id' | 'driver_id' | ...
    first_seen_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    retry_count         SMALLINT        NOT NULL DEFAULT 0,
    last_retry_at       TIMESTAMP
);


-- ============================================================
-- INDEXES
-- ============================================================

-- fact_order
CREATE INDEX idx_fact_order_customer    ON fact_order(customer_id);
CREATE INDEX idx_fact_order_restaurant  ON fact_order(restaurant_id);
CREATE INDEX idx_fact_order_driver      ON fact_order(driver_id);
CREATE INDEX idx_fact_order_date        ON fact_order(date_key);
CREATE INDEX idx_fact_order_status      ON fact_order(order_status);
CREATE INDEX idx_fact_order_created     ON fact_order(created_at);

-- fact_ticket
CREATE INDEX idx_fact_ticket_order      ON fact_ticket(order_id);
CREATE INDEX idx_fact_ticket_agent      ON fact_ticket(agent_id);
CREATE INDEX idx_fact_ticket_priority   ON fact_ticket(priority_id);
CREATE INDEX idx_fact_ticket_date       ON fact_ticket(date_key);
CREATE INDEX idx_fact_ticket_status     ON fact_ticket(status);
CREATE INDEX idx_fact_ticket_sla_r      ON fact_ticket(sla_response_breached);
CREATE INDEX idx_fact_ticket_sla_res    ON fact_ticket(sla_resolution_breached);
CREATE INDEX idx_fact_ticket_region     ON fact_ticket(region_id);

-- fact_ticket_event
CREATE INDEX idx_ticket_event_ticket    ON fact_ticket_event(ticket_id);
CREATE INDEX idx_ticket_event_ts        ON fact_ticket_event(event_ts);

-- dim lookups
CREATE INDEX idx_dim_customer_region    ON dim_customer(region_id);
CREATE INDEX idx_dim_driver_region      ON dim_driver(region_id);
CREATE INDEX idx_dim_restaurant_region  ON dim_restaurant(region_id);

-- pipeline log
CREATE INDEX idx_quality_log_date       ON pipeline_quality_log(stream_date);
CREATE INDEX idx_quality_log_file       ON pipeline_quality_log(source_file);

-- quarantine
CREATE INDEX idx_quarantine_reason      ON quarantine(rejection_reason);
CREATE INDEX idx_quarantine_at          ON quarantine(quarantined_at);

-- true_orphan
CREATE INDEX idx_true_orphan_fk         ON true_orphan(unresolved_fk);


-- ============================================================
-- SEED: Static lookup dimensions
-- ============================================================

-- dim_priority
INSERT INTO dim_priority (priority_id, priority_code, priority_name, sla_first_response_min, sla_resolution_min) VALUES
    (1, 'P1', 'Critical', 1,  15),
    (2, 'P2', 'High',     1,  15),
    (3, 'P3', 'Medium',   1,  15),
    (4, 'P4', 'Low',      1,  15);

-- dim_channel
INSERT INTO dim_channel (channel_id, channel_name) VALUES
    (1, 'app'),
    (2, 'chat'),
    (3, 'phone'),
    (4, 'email');

-- dim_reason
INSERT INTO dim_reason (reason_id, reason_name, reason_category, severity_level, typical_refund_pct) VALUES
    (1,  'Late Delivery',             'Delivery', 3, 0.15),
    (2,  'Wrong Item',                'Food',     3, 0.50),
    (3,  'Missing Items',             'Food',     2, 0.30),
    (4,  'Cold Food',                 'Food',     2, 0.25),
    (5,  'Payment Issue',             'Payment',  3, 1.00),
    (6,  'Driver Behavior',           'Delivery', 4, 0.20),
    (7,  'Order Never Arrived',       'Delivery', 5, 1.00),
    (8,  'Poor Food Quality',         'Food',     2, 0.40),
    (9,  'Packaging Damaged',         'Delivery', 1, 0.10),
    (10, 'Allergic Reaction Concern', 'Food',     5, 1.00);

-- dim_region (Cairo, Giza, Alexandria — from generate_master_data.py)
INSERT INTO dim_region (region_id, region_name, city_name, delivery_base_fee) VALUES
    (1,  'Maadi',         'Cairo',       16.89),
    (2,  'Nasr City',     'Cairo',       13.94),
    (3,  'Heliopolis',    'Cairo',       15.76),
    (4,  'Downtown',      'Cairo',       15.75),
    (5,  'New Cairo',     'Cairo',       17.61),
    (6,  'Zamalek',       'Cairo',       14.37),
    (7,  'Dokki',         'Cairo',       14.57),
    (8,  '6th October',   'Giza',        15.86),
    (9,  'Sheikh Zayed',  'Giza',        14.85),
    (10, 'Haram',         'Giza',        16.59),
    (11, 'Faisal',        'Giza',        14.12),
    (12, 'Mohandessin',   'Giza',        19.23),
    (13, 'Smouha',        'Alexandria',  10.07),
    (14, 'Gleem',         'Alexandria',  11.28),
    (15, 'Miami',         'Alexandria',  11.32),
    (16, 'Montaza',       'Alexandria',  13.95),
    (17, 'Sidi Gaber',    'Alexandria',  11.67),
    (18, 'Stanley',       'Alexandria',  13.74);


-- ============================================================
-- HELPER: Populate dim_date for 2019-01-01 → 2030-12-31
-- ============================================================
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    week, day_of_week, day_of_month, day_of_year, day_name
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER         AS date_key,
    d::DATE                                  AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT        AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT        AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT        AS month,
    TO_CHAR(d, 'Month')                      AS month_name,
    EXTRACT(WEEK    FROM d)::SMALLINT        AS week,
    EXTRACT(DOW     FROM d)::SMALLINT + 1    AS day_of_week,   -- 1=Sun … 7=Sat
    EXTRACT(DAY     FROM d)::SMALLINT        AS day_of_month,
    EXTRACT(DOY     FROM d)::SMALLINT        AS day_of_year,
    TO_CHAR(d, 'Day')                        AS day_name
FROM generate_series(
    '2019-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS t(d);


-- ============================================================
-- HELPER: Populate dim_time for every minute of the day
-- ============================================================
INSERT INTO dim_time (time_key, full_time, hour, minute, am_pm, shift)
SELECT
    (h * 100 + m)                            AS time_key,    -- HHMM
    MAKE_TIME(h, m, 0)                       AS full_time,
    h::SMALLINT                              AS hour,
    m::SMALLINT                              AS minute,
    CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
    CASE
        WHEN h BETWEEN 6  AND 13 THEN 'morning'
        WHEN h BETWEEN 14 AND 21 THEN 'evening'
        ELSE                           'night'
    END                                      AS shift
FROM
    generate_series(0, 23) AS h,
    generate_series(0, 59) AS m;
