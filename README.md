# FastFeastETL

A production-grade, near real-time data pipeline built for **FastFeast* — a rapidly growing food delivery platform. The pipeline extracts batch and micro-batch data from OLTP exports, validates and transforms it, loads it into a PostgreSQL dimensional model, runs OLAP analytics, efficiently processing both batch and micro-batch operational data. It extracts data from OLTP exports, performs comprehensive validation and transformation, loads the refined data into a PostgreSQL-based dimensional model, executes OLAP analytics, and ultimately feeds a Power BI dashboard for insightful visualization.


**Key Features**:
- 🎯 **Configuration-driven**: All behavior defined in YAML (no code changes to add files)
- ⚡ **Fast validation**: In-memory schema checks before any DB writes
- 🔒 **Data quality**: Multi-stage validation (schema → records → orphans)
- 🛡️ **PII safe**: SHA-256 hashing of sensitive columns
- 📊 **Full audit trail**: Every file logged to Postgres (success, failure, quarantine counts)
- 🚨 **Smart alerting**: SMTP notifications for critical failures
- 📈 **SLA tracking**: Automatic SLA metrics for support tickets (background job)
- ♻️ **Idempotent**: MD5-based deduplication, safe to replay files

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Directory Structure](#directory-structure)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [Module Guide](#module-guide)
6. [Data Flow](#data-flow)
7. [Validation Pipeline](#validation-pipeline)
8. [Quarantine & Error Handling](#quarantine--error-handling)
9. [SLA Tracking](#sla-tracking)
10. [Monitoring & Operations](#monitoring--operations)
11. [Logging & Alerting](#logging--alerting)

---

## Architecture Overview

### Core Principles

- **Single Source of Truth**: Configuration drives all behavior via `pipeline_config.yaml`
- **No Hardcoding**: Schema definitions, file mappings, FK rules all come from config
- **Idempotent**: Every file has an MD5 hash; processing the same file twice is a no-op
- **Fast Validation**: Schema validation happens in-memory before any DB writes
- **Quarantine First**: Bad records captured at every stage (schema, records, orphan)
- **PII Safe**: Sensitive columns hashed with SHA-256 before storage
- **Scalable**: Thread pools, async watchers, non-blocking trackers

### High-Level Flow

```
Input Files (batch/stream)
  ↓
[File Watcher] — detects new files (batch @ trigger_hour, stream continuous)
  ↓
[Ingestion Runner] — reads, hashes, checks tracker
  ↓
[Schema Validator] — columns, datatypes, nulls, categoricals
  ↓
[Record Validators] — batch: nulls/dupes/formats/ranges | stream: same + empty values
  ↓
[Orphan Validator] — stream only, FK constraints via fk_checker
  ↓
[PII Handler] — masks sensitive columns
  ↓
[Dim/Fact Loaders] — INSERT into DWH with ON CONFLICT safety
  ↓
[Quarantine] — rejected rows stored as JSONB + CSV backup
  ↓
[File Log] — analytics-facing audit trail in Postgres
```

---

## Directory Structure

```
FastFeastETL/
├── config/
│   └── pipeline_config.yaml          # SINGLE SOURCE OF TRUTH
│   ├── config_loader.py              # YAML config loading
│   └── .env                          # Secrets (PGPASSWORD, SMTP_*, etc.)
│
├── ingestion/
│   ├── ingestion_runner.py           # Main orchestrator, entry point
│   ├── file_watcher.py               # Batch + stream watchers (threads)
│   ├── file_reader.py                # CSV/JSON parser
│   ├── file_tracker.py               # SQLite idempotency via MD5
│
├── validation/
│   ├── validation_runner.py          # Orchestrator (delegates to validators)
│   ├── schema_validator.py           # Columns, dtypes, nulls, categoricals
│   ├── schema_registry.py            # Builds Schema objects from config
│   ├── batch_records_validator.py    # Null/dup/format/range checks
│   ├── stream_records_validator.py   # Null/dup/empty/format/range checks
│   ├── orphan_validator.py           # FK constraint checks (stream only)
│   ├── pii_handler.py                # SHA-256 masking for sensitive columns
│   └── fault_handler.py              # Quarantine writer (CSV + Postgres)
│
├── datawarehouse/
│   ├── db_connection.py              # Thread pool, bootstrap DB
│   ├── schema_init.py                # DDL for all tables + indexes
│   ├── dim_loader.py                 # INSERT into dimension tables
│   ├── fact_loader.py                # INSERT into fact tables + SLA calcs
│   ├── fk_checker.py                 # Postgres FK lookups for orphan checks
│   ├── quarantine_loader.py          # Write rejected rows to Postgres
│   └── file_log.py                   # Write audit trail to pipeline_file_log
│   └── master_seeder.py              # Cold-start dimension loader
|   ├── quarantine_retry.py           # For true orphans
│
├── utils/
│   ├── logger.py                     # Structured logging
│   └── alerter.py                    # SMTP notifications
│   ├── sla_updater_job.py            # calculating SLA metrics from fact_ticket

├── scripts/                                 
│   ├── generate_master_data.py       # Initialize dimensions
│   ├── generate_batch_data.py        # Daily batch snapshots
│   ├── generate_stream_data.py       # Hourly stream events
│   ├── add_new_customers.py          # Add customers (orphan test)
│   ├── add_new_drivers.py            # Add drivers (orphan test)
│   ├── simulate_day.py               # Full day simulation
│
├── data/
│   ├── master/                       # Master dimension files (cold-start)
│   ├── input/batch/                  # Batch input (YYYY-MM-DD/)
│   ├── input/stream/                 # Stream input (YYYY-MM-DD/HH/)
│   ├── quarantine/                   # CSV backup of rejected records
│   ├── tracker/                      # SQLite idempotency DB
│   └── logs/                         # Application logs (pipeline_YYYY-MM-DD.log)
│
└── main.py                           # Entry point (start/stop)
└── README.md                         # This file
└── README_datawarehouse.md   
```

---

## Quick Start

### Prerequisites

- **Python**: 3.10+
- **PostgreSQL**: 12+ (or compatible)
- **Required packages**: `pandas`, `psycopg2`, `pyyaml`, `watchdog`, `sqlalchemy`

### Installation

```bash
git clone <repo>
cd FastFeastETL
pip install -r requirements.txt
```

### Environment Setup

1. **Copy environment template**:
   ```bash
   cp env.example .env
   ```

2. **Edit `.env` with your credentials**:
   ```bash
   # .env (DO NOT COMMIT — listed in .gitignore)
   PGPASSWORD=your_postgres_password
   SMTP_USER=your_email@gmail.com
   SMTP_PASSWORD=your_16_char_app_password     # from https://myaccount.google.com/apppasswords
   SMTP_SENDER=your_email@gmail.com
   SMTP_RECEIVER=team@fastfeast.com
   ```

   > **Note**: For Gmail, use an [App Password](https://myaccount.google.com/apppasswords), not your account password.

3. **Verify `.env` is ignored**:
   ```bash
   # .gitignore already includes:
   *.env
   !.env.example
   
   # So .env is never committed, only .env.example
   ```

### Configuration

1. **Review `pipeline_config.yaml`**:
   - Database credentials (uses `${PGPASSWORD}` from `.env`)
   - SMTP settings (uses `${SMTP_*}` from `.env`)
   - File watchers (batch directory, stream directory, trigger_hour)
   - Expected files and their target DWH tables
   - Schemas for all source files

2. **Create input directories**:
   ```bash
   mkdir -p data/input/batch data/input/stream data/master data/quarantine data/tracker logs
   ```

3. **Generate Test Data**:
   ```bash
   python scripts/generate_master_data.py
   python scripts/generate_batch_data.py --date 2026-02-20
   python scripts/generate_stream_data.py --date 2026-02-20 --hour 9
   ```

### Running the Pipeline

```bash
python main.py
```

**Console output**:
```
=====================================================
🚀 FastFeast Pipeline is running! Waiting for files...
Press Ctrl+C to stop the pipeline gracefully.
=====================================================
```

**What happens**:
1. ✓ Loads and validates `pipeline_config.yaml`
2. ✓ Initializes Postgres connection pool
3. ✓ Creates all DWH tables (if not exist)
4. ✓ Seeds master dimensions from `data/master/`
5. ✓ Starts SLA updater background job (every 10 min)
6. ✓ Starts batch watcher (scans daily @ `trigger_hour`)
7. ✓ Starts stream watcher (continuous, watchdog-based)
8. ✓ Blocks on file queue, processing files as they arrive

**Graceful shutdown**:
```bash
Ctrl+C
# Output:
# 🛑 Stop signal received (Ctrl+C).
# Shutting down background threads... please wait.
# 👋 Pipeline stopped. Goodbye!
```

All background threads close cleanly, files in-flight are finished, and the database pool is drained.

---

## Configuration

All behavior is controlled by `pipeline_config.yaml`. No code changes needed to add a file or modify a schema.

### Config Structure

#### Database
```yaml
database:
  host:     localhost
  port:     5432
  user:     postgres
  password: ${PGPASSWORD}          # ENV VAR from .env or shell
  dbname:   fastfeastapp
```

#### Watchers
```yaml
watcher:
  batch:
    dir: "data/input/batch"
    trigger_hour: 0                 # UTC hour to scan for daily batch
    expected_files:
      customers.csv:
        table: "Dim_Customer"       # DWH table name
      orders.json:
        table: "Fact_Order"
  stream:
    dir: "data/input/stream"
    expected_files:
      orders.json:
        table: "Fact_Order"
        foreign_keys:               # stream only
          customer_id: customers
          driver_id: drivers
```

#### Tracker & Reader
```yaml
tracker:
  db_path: "data/tracker/pipeline_tracker.db"

reader:
  file_type_map:                    # filename → logical type
    customers.csv:  "customers"
    orders.json:    "orders"
    # No hardcoding here — fully config-driven
```

#### Schemas
```yaml
schemas:
  customers:
    primary_key: customer_id
    columns:
      - { name: customer_id, dtype: int,  nullable: false }
      - { name: full_name,   dtype: str,  nullable: true  }
      - { name: email,       dtype: str,  nullable: true  }
      - { name: gender,      dtype: str,  nullable: true, 
          allowed_values: [male, female] }
```

**Schema Keywords**:
- `dtype`: `str | int | float | bool | datetime`
- `nullable`: `true | false` (false = NOT NULL)
- `allowed_values`: optional list of valid values (whitelist)

#### Alerting
```yaml
alerting:
  enabled: True
  smtp:
    host: "smtp.gmail.com"
    port: 587
    user_name: ${SMTP_USER}
    password: ${SMTP_PASSWORD}
    sender: ${SMTP_SENDER}
    receivers: ["team@fastfeast.com"]
```

---

## Module Guide

## Module Guide

### Entry Point

#### `main.py`
The single entry point for the entire pipeline. Handles graceful startup and shutdown.

**Flow**:
```python
main()
  ├─ Start SLA updater thread (background, daemon=False for clean exit)
  ├─ Call ingestion_runner.start() — blocks here
  │   └─ Initialize DB, seed master, start watchers, run file loop
  ├─ On Ctrl+C or error: catch exception
  └─ Call ingestion_runner.stop() — stops all watchers, flushes tracker
```

**Key features**:
- Graceful shutdown via `KeyboardInterrupt` (Ctrl+C)
- SLA updater runs in background (every 10 min)
- All exceptions caught and logged before exit
- Clean teardown of threads and connections

**Usage**:
```bash
python main.py
# Output:
# =====================================================
# 🚀 FastFeast Pipeline is running! Waiting for files...
# Press Ctrl+C to stop the pipeline gracefully.
# =====================================================
```

### Utilities Layer

#### `utils/logger.py`
Structured JSON logging with single-threaded file writer.

**Key functions**:
- `get_logger(name)` — called once per module at module level
- `shutdown()` — flushes remaining records, closes file (called from main.py)

**Architecture**:
- Main thread + worker threads → put LogRecords into queue (non-blocking)
- QueueListener thread → reads queue, writes to file (single writer, no interleaving)
- Every record formatted as one JSON line

**Example**:
```python
from utils.logger import get_logger

logger = get_logger(__name__)
logger.info(f"[runner] processed {count} files", extra={"count": count})

# Output to logs/pipeline_2026-02-22.log:
# {"time": "2026-02-22 10:23:45", "level": "INFO", "module": "ingestion.ingestion_runner", "msg": "[runner] processed 5 files", "count": 5}
```

#### `utils/alerter.py`
SMTP email alerts for critical pipeline failures.

**Key function**:
- `send_alert(error, message)` — spawns background thread, never blocks

**When to alert**:
```python
from utils.alerter import send_alert

# Critical failure
send_alert(
    error="Schema Validation failure",
    message="Required columns don't exist in orders.json"
)
```

**Configuration**:
- All SMTP settings from config + .env
- Alerting can be disabled (set `enabled: False`)
- Non-blocking (background thread handles email)
- Failures logged but don't crash pipeline

#### `utils/sla_updater_job.py`
Background job that updates SLA performance view every 10 minutes.

**Key function**:
- `sla_scheduler_loop(stop_event)` — runs in daemon thread

**How it works**:
1. Connects to Postgres via SQLAlchemy
2. Executes CREATE OR REPLACE VIEW (idempotent)
3. Sleeps 10 minutes (in 1-second intervals for responsive shutdown)
4. Wakes up every second to check stop_event
5. Exits cleanly when pipeline stops

**View created**:
```sql
v_ticket_sla_performance — SLA metrics for each ticket
  - is_first_response_met (BOOLEAN)
  - is_resolution_met (BOOLEAN)
  - timestamps and duration fields
```

### Ingestion Layer

#### `ingestion_runner.py`
Main orchestrator. Implements the pipeline loop: read → validate → load → track.

**Key functions**:
- `start()` — initialize DWH, seed master, start watchers
- `stop()` — graceful shutdown
- `_process_file()` — single-file pipeline
- `_run_loop()` — main blocking loop

**Responsibilities**:
- Route files to validation_runner
- Dispatch to dim_loader (batch) or fact_loader (stream)
- Call file_tracker.mark_as_done/mark_as_failed
- Log audit trail to pipeline_file_log

#### `file_watcher.py`
Daemon threads for detecting new files.

**Batch Watcher**: Scans `data/input/batch/YYYY-MM-DD/` at `trigger_hour` daily. FK-safe load order enforced (parent dims before children).

**Stream Watcher**: Continuously monitors `data/input/stream/` via watchdog, instant detection.

#### `file_reader.py`
Reads CSV/JSON into DataFrames. No type coercion — all columns start as `str` so validation layer decides nulls.

**Key functions**:
- `read_file(path)` → `(DataFrame, file_type_name)`
- `get_file_type(filename)` → validates file is in FILE_TYPE_MAP

#### `file_tracker.py`
Single SQLite thread for idempotent processing. Computes MD5 hash, checks if already processed, writes success/failure record.

**Why SQLite**: Fast, simple, no network overhead. Postgres pipeline_file_log is separate (analytics-facing audit).

**Key functions**:
- `compute_hash(file_path)` → MD5 hex string
- `is_processed(file_path, file_hash)` → bool (non-blocking via Future)
- `mark_as_done/mark_as_failed()` → fire-and-forget

#### `master_seeder.py`
Runs once at startup. Reads `data/master/` files (CSV format), validates, loads into DWH before any watchers start.

Handles special case: master restaurants/cities are `.csv` (OLTP format) but are validated as logical type `restaurants`/`cities`.

---

### Validation Layer

#### `validation_runner.py`
Orchestrator. Delegates to schema → record → (orphan) validators, calls PII handler, writes quarantine.

**Flow**:
1. Schema validation
2. Batch-specific or stream-specific record validation
3. Stream only: orphan validation
4. PII masking
5. Return clean DataFrame + orphan count

#### `schema_validator.py`
Validates columns exist, datatypes match, nulls enforced, categoricals in whitelist.

**Key insight**: file_reader reads everything as `str`, so `schema_validator` uses `pd.to_numeric`, `pd.to_datetime` with `errors='coerce'` to convert safely. Null-equivalent strings (`"None"`, `"nan"`, `""`) are passed through so `validate_nulls` can enforce NOT NULL constraints per schema.

#### `batch_records_validator.py`
Validates batch dimensions for:
- Nulls (reject rows with any null in required columns)
- Empty/placeholder values (`""`, `"nan"`, `"n/a"`)
- Duplicates (keep first, quarantine rest)
- Date formats (`created_at`, `updated_at`, etc.)
- Email regex (`.com` domains)
- Phone regex (Egyptian 10-11 digit)
- National ID length (15 chars)
- Numeric ranges (age ≥ 0, rating ≤ 5, rates ≤ 1, etc.)

#### `stream_records_validator.py`
Same as batch, plus:
- Ignores `old_status` for null checks (first event has no prior state)
- Validates monetary amounts ≥ 0 (order_amount, refund_amount, etc.)

#### `orphan_validator.py`
FK constraint checks for stream facts. Uses `fk_checker` to:
1. Get FK rules from config (e.g., `orders.json` has `customer_id → customers`)
2. Query Postgres for valid parent IDs
3. Return (clean, orphans) split

**Why Postgres**: Master data is already loaded; one source of truth for PK values.

#### `schema_registry.py`
Builds `Schema` dataclass objects from `pipeline_config.yaml` on demand. No hardcoding.

```python
registry = schema_registry()
schema = registry.get_schema("customers")  # ← returns Schema or None
```

#### `pii_handler.py`
SHA-256 hashes sensitive columns before load. Configurable per file:

```python
self.pii_columns = {
    "customers": ["email", "phone"],
    "drivers": ["driver_phone", "national_id"],
    "agents": ["agent_email", "agent_phone"]
}
```

Null/NaN values are left unchanged (not hashed).

#### `fault_handler.py`
Writes rejected rows to:
1. **Postgres `quarantine` table** (JSONB raw_data) — queryable, analytics-ready
2. **CSV backup** `data/quarantine/{file_type}/quarantined_YYYY-MM-DD.csv` — spreadsheet audit

Determines `rejection_stage`:
- `'schema'` — column/dtype/null/categorical failure
- `'records'` — dup/format/range failure
- `'orphan'` — FK constraint failure

---

### Data Warehouse Layer

#### `db_connection.py`
Single source of truth for all Postgres connections. Thread pool management.

**Key functions**:
- `init_pool()` — creates DB if needed, opens ThreadedConnectionPool (min=2, max=10)
- `get_conn()` / `put_conn()` — borrow/return connection from pool
- `close_pool()` — graceful shutdown

All other DWH modules import from here — never build their own engines.

#### `schema_init.py`
Creates all tables on startup (IF NOT EXISTS, idempotent).

**Key tables**:
- `dim_*` (region, segment, customer, driver, agent, restaurant, etc.)
- `fact_order`, `fact_ticket`, `fact_ticket_event`
- `dim_date`, `dim_time` (pre-populated)
- `quarantine` (JSONB — works for any rejected record)
- `true_orphan` (after 24h retry window, populated by your stored procedure)
- `pipeline_file_log` (analytics-facing audit trail)

**Foreign Keys**: Enforced on all fact tables and cross-dim refs (e.g., `dim_reason → dim_reason_category`).

#### `dim_loader.py`
Loads validated dimension DataFrames into DWH.

**Strategy**: ON CONFLICT DO NOTHING (safety net; validation guarantees PK uniqueness).

#### `fact_loader.py`
Loads validated fact DataFrames + computes analytics-ready columns:

**For `fact_order`**:
- `date_key` → YYYYMMDD integer from `order_created_at`
- `time_key` → HHMM integer from `order_created_at`

**For `fact_ticket`**:
- `date_key`, `time_key` from `created_at`
- `first_response_min` — minutes from created_at → first_response_at
- `resolution_min` — minutes from created_at → resolved_at
- `sla_response_breached` — actual > sla_first_due_at
- `sla_resolution_breached` — actual > sla_resolve_due_at
- `reopened` — boolean flag (status == 'Reopened')
- `region_id` — derived by looking up ticket's order_id in fact_order

**For `fact_ticket_event`**:
- `date_key`, `time_key` from `event_ts`

#### `fk_checker.py`
Replaces SQLite `reference.db`. All FK lookups query Postgres directly.

**Key functions**:
- `check_fk(df, fk_col, ref_table, ref_pk)` → `(clean_df, orphan_df)`
- `resolve_ref_table(logical_name)` → `(table_name, pk_col)` — maps config FK name to table
- `_normalize_id()` — handles float 370.0 → string "370" mismatch

**Why this matters**: schema_validator coerces `int` columns to `float`, so `370` becomes `370.0` → `"370.0"` when stringified. Postgres PK is `370` → `"370"`. Mismatch = false orphans. Fix: strip trailing `.0` before comparison.

#### `quarantine_loader.py`
Writes rejected rows to Postgres `quarantine` table (JSONB column for any table shape).

#### `file_log.py`
Writes one row to `pipeline_file_log` per processed file (success or failure).

**Analytics-ready columns**:
- `file_name`, `file_hash`, `file_type` (batch/stream)
- `source_table`, `status` (success/failed)
- `total_records`, `valid_records`, `quarantined`
- `duplicate_count`, `orphan_count`
- `rejection_stage` (schema/records/orphan or null)

---

## Data Flow

### Batch (Dimension) Files

1. **File Watcher**: Batch thread scans `data/input/batch/YYYY-MM-DD/` at `trigger_hour` (0 UTC by default)
2. **Enqueue**: Queues files in FK-safe order (parents before children)
3. **Ingestion Runner**:
   - Reads file (CSV)
   - Computes MD5 hash
   - Checks SQLite tracker — skip if already processed
4. **Validation**:
   - Schema validation (columns, dtypes, nulls, categoricals)
   - Batch records validation (nulls, empty, dupes, formats, ranges)
   - Rejected rows → quarantine (CSV + Postgres)
5. **Load**:
   - PII masking (email, phone)
   - INSERT into dimension table
   - ON CONFLICT DO NOTHING safety
6. **Track**:
   - Mark as done in SQLite tracker
   - Log audit row to pipeline_file_log

### Stream (Fact) Files

1. **File Watcher**: Stream thread monitors `data/input/stream/YYYY-MM-DD/HH/` continuously (watchdog)
2. **Enqueue**: Instant detection, queue file
3. **Ingestion Runner**:
   - Reads file (JSON)
   - Computes MD5 hash
   - Checks SQLite tracker
4. **Validation**:
   - Schema validation
   - Stream records validation (nulls, empty, dupes, formats, ranges)
   - **Orphan validation** — FK checks via fk_checker:
     - For each FK in config (e.g., `customer_id → customers`)
     - Query Postgres for valid customer IDs
     - Split into (clean, orphans)
   - Rejected rows → quarantine (all stages)
5. **Load**:
   - PII masking (none for orders/tickets, but extensible)
   - INSERT into fact table
   - Compute SLA metrics, region lookups, time keys
6. **Track**:
   - Mark as done
   - Log audit row

---

## Validation Pipeline

### Schema Validation

Ensures structure correctness before any business logic.

**Checks**:
1. **Columns**: Exact match (no missing, no extra)
2. **Datatypes**: Convert str → int/float/datetime, reject if unreadable
3. **Nulls**: Reject rows with null in NOT NULL columns
4. **Categoricals**: Whitelist enforcement (e.g., `gender` ∈ {male, female})

**Output**: Valid DataFrame + rejected DataFrame

### Record Validation (Batch)

1. **Nulls**: Drop rows with any null
2. **Empty**: Drop rows with "", "nan", "null", "n/a"
3. **Duplicates**: Keep first, quarantine rest by PK
4. **Formats**:
   - Dates: `created_at`, `updated_at`, `signup_date`, `hire_date`
   - Emails: `.com` domains
   - Phones: Egyptian regex (10-11 digit)
   - National ID: exactly 15 chars
5. **Ranges**:
   - Numeric ≥ 0: segment_id, team_id, ratings, rates, prep times
   - Ratings ≤ 5
   - Rates ≤ 1

### Record Validation (Stream)

Same as batch, plus:
- **Empty**: drop "", "nan", "null", "none", "n/a"
- **Ignores**: `old_status` for null checks (first event has no prior state)

### Orphan Validation (Stream Only)

After records pass validation, FK constraints are checked.

**For each fact file**:
- Read FK rules from config (e.g., orders.json has `customer_id → customers`)
- Query Postgres: which customer IDs exist in `dim_customer`?
- Compare fact data against valid IDs
- Quarantine mismatches as `'orphan'` rejection_stage

**24-hour retry window**: You provide a stored procedure to promote true orphans after 24h (data may arrive out-of-order).

---

## Quarantine & Error Handling

### Quarantine Strategy

Every rejected record is captured at the source, with reason + stage:

```sql
SELECT 
  quarantine_id,
  source_file,
  source_table,
  rejection_reason,
  rejection_stage,    -- 'schema' | 'records' | 'orphan'
  raw_data,
  quarantined_at
FROM quarantine
WHERE quarantined_at > NOW() - INTERVAL '24 hours'
ORDER BY quarantined_at DESC;
```

**Output destinations**:
1. **Postgres `quarantine` table** (JSONB) — queryable by dashboards
2. **CSV backup** `data/quarantine/{batch|stream}/quarantined_YYYY-MM-DD.csv` — spreadsheet audit

### True Orphan Promotion

After 24 hours, a stored procedure (you write) can promote orphans:

```sql
INSERT INTO true_orphan 
  (quarantine_id, source_file, source_table, unresolved_fk, raw_data, first_seen_at)
SELECT 
  quarantine_id, source_file, source_table, 'customer_id',
  raw_data, quarantined_at
FROM quarantine
WHERE rejection_stage = 'orphan'
  AND source_table = 'fact_order'
  AND quarantined_at < NOW() - INTERVAL '24 hours';
```

Example file: `stored_procedure_true_orphans.sql` (provided).

### Error Handling & Alerts

- **Schema failures** → alert, skip file (data too broken)
- **Record failures** → quarantine, continue (partial load)
- **DB errors** → alert, retry on next run (idempotent)
- **High quarantine volume** → alert if >50 records in one file

---

## SLA Tracking

The pipeline includes an automatic SLA updater that runs every 10 minutes in a background thread.

### SLA View

The `sla_updater_job.py` creates/updates a materialized view:

```sql
CREATE OR REPLACE VIEW v_ticket_sla_performance AS
SELECT
  ticket_id,
  agent_id,
  created_at,
  first_response_at,
  sla_first_due_at,
  resolved_at,
  sla_resolve_due_at,
  CASE
    WHEN first_response_at <= sla_first_due_at THEN TRUE
    ELSE FALSE
  END AS is_first_response_met,
  CASE
    WHEN resolved_at <= sla_resolve_due_at THEN TRUE
    ELSE FALSE
  END AS is_resolution_met
FROM fact_ticket;
```

### SLA Metrics Queries

**SLA compliance by agent**:
```sql
SELECT
  agent_id,
  COUNT(*) as total_tickets,
  SUM(CASE WHEN is_first_response_met THEN 1 ELSE 0 END) as first_response_met,
  SUM(CASE WHEN is_resolution_met THEN 1 ELSE 0 END) as resolution_met,
  ROUND(100.0 * SUM(CASE WHEN is_first_response_met THEN 1 ELSE 0 END) 
    / NULLIF(COUNT(*), 0), 2) as first_response_rate_pct
FROM v_ticket_sla_performance
WHERE DATE(created_at) = CURRENT_DATE
GROUP BY agent_id
ORDER BY first_response_rate_pct DESC;
```

**Tickets breached SLA**:
```sql
SELECT
  ticket_id,
  agent_id,
  created_at,
  sla_first_due_at,
  first_response_at,
  CASE WHEN first_response_at > sla_first_due_at 
    THEN EXTRACT(MINUTE FROM (first_response_at - sla_first_due_at))
    ELSE 0
  END as minutes_over_sla
FROM v_ticket_sla_performance
WHERE NOT is_first_response_met
  AND DATE(created_at) = CURRENT_DATE
ORDER BY minutes_over_sla DESC;
```

### Background Job Details

**File**: `utils/sla_updater_job.py`

**How it works**:
- Main thread starts SLA updater as daemon thread
- Updater connects to Postgres with SQLAlchemy
- Executes CREATE OR REPLACE VIEW (idempotent)
- Sleeps 10 minutes (in 1-second intervals for responsive shutdown)
- Stops cleanly when main thread exits

**Monitoring the job**:
```bash
# Check logs for SLA updates:
grep "SLA view updated" logs/*.log

# If failures:
grep "SLA view update failed" logs/*.log
```

---

## Monitoring & Operations

### Logs

The pipeline uses **structured JSON logging** for easy parsing and shipping to any log aggregation tool.

**Logger**: `utils/logger.py`

**Features**:
- Single-threaded file writer via QueueListener (no interleaving)
- JSON format, one record per line
- Automatic `logs/pipeline_YYYY-MM-DD.log` naming
- All threads put records into queue (non-blocking)

**Log format**:
```json
{
  "time": "2026-02-22 06:01:23",
  "level": "ERROR",
  "module": "validation.orphan_validator",
  "msg": "Found 5 orphans on 'customer_id' → dim_customer.customer_id",
  "file_name": "orders.json",
  "count": 5
}
```

**View logs**:
```bash
# All logs for today:
cat logs/pipeline_2026-02-22.log

# Errors only:
grep '"level": "ERROR"' logs/*.log | jq .

# From a specific module:
grep '"module": "ingestion.fact_loader"' logs/*.log

# SLA updates:
grep "SLA view updated" logs/*.log
```

---

## Logging & Alerting

### Email Alerts (SMTP)

The pipeline sends email alerts for critical failures.

**Alerter**: `utils/alerter.py`

**Configuration** (in `pipeline_config.yaml`):
```yaml
alerting:
  enabled: True
  smtp:
    host: "smtp.gmail.com"
    port: 587
    user_name: ${SMTP_USER}           # from .env
    password: ${SMTP_PASSWORD}        # from .env (Gmail App Password)
    sender: ${SMTP_SENDER}            # from .env
    receivers:                        # list of emails
      - team@fastfeast.com
```

**Alert triggers**:
- Schema validation critical failure (entire file rejected)
- DWH load error
- High quarantine volume (>50 records in one file)
- Pipeline exception/crash

**Example alert email**:
```
Subject: Alert: Schema Validation failure

Hello,

An alert has been triggered in the FastFeast pipeline.

Error: Schema Validation failure

Details:
Required Columns don't exist in orders.json

--
This is an automated alert from your reliable Data Pipeline.
```

**How it works**:
- Alert triggered in pipeline code: `send_alert(error="...", message="...")`
- Spawns background thread (non-blocking)
- Thread connects to Gmail SMTP, authenticates, sends email
- Failures logged but never crash the pipeline

**Disable alerting**:
```yaml
alerting:
  enabled: False
```

**Gmail App Password setup**:
1. Go to https://myaccount.google.com/apppasswords
2. Select "Mail" and "Windows Computer"
3. Google generates a 16-character password
4. Copy it to `.env` as `SMTP_PASSWORD`
5. Never use your real Gmail password in the code!

### Logger Shutdown

The logger's QueueListener is closed cleanly on graceful shutdown (via `main.py`):

```python
# main.py
finally:
    ingestion_runner.stop()
    logger_shutdown()  # flushes remaining records and closes file
    print("👋 Pipeline stopped.")
```

This ensures no log records are lost during shutdown.

---

## File Log Queries

**Daily summary**:
```sql
SELECT 
  DATE(processed_at) as date,
  file_type,
  COUNT(*) as files_processed,
  SUM(total_records) as total_records,
  SUM(valid_records) as valid_records,
  SUM(quarantined) as quarantined_total,
  SUM(orphan_count) as orphans_total,
  COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files
FROM pipeline_file_log
WHERE processed_at > NOW() - INTERVAL '1 day'
GROUP BY DATE(processed_at), file_type;
```

**Failed files**:
```sql
SELECT file_name, source_table, rejection_stage, quarantined, processed_at
FROM pipeline_file_log
WHERE status = 'failed'
ORDER BY processed_at DESC
LIMIT 20;
```

**Duplicate metrics**:
```sql
SELECT 
  file_name,
  SUM(duplicate_count) as total_dupes,
  ROUND(100.0 * SUM(duplicate_count) / NULLIF(SUM(total_records), 0), 2) as dup_rate_pct
FROM pipeline_file_log
WHERE processed_at > NOW() - INTERVAL '7 days'
GROUP BY file_name
ORDER BY total_dupes DESC;
```

### Logs

Structured JSON logs in `logs/` (via `utils/logger.py`):

```json
{
  "timestamp": "2024-01-15T10:23:45.123Z",
  "level": "ERROR",
  "logger": "validation.orphan_validator",
  "message": "Found 5 orphans on 'customer_id' → dim_customer.customer_id",
  "extra": {
    "file_name": "orders.json",
    "count": 5
  }
}
```

### Health Checks

**Pipeline startup**:
```bash
python -c "
from config.config_loader import get_config
from datawarehouse.db_connection import init_pool, get_conn, put_conn
from datawarehouse.schema_init import init_schema

cfg = get_config()
print('✓ Config loaded')
init_pool()
print('✓ DB pool initialized')
conn = get_conn()
put_conn(conn)
print('✓ DB connection OK')
init_schema()
print('✓ Schema verified')
print('Pipeline ready')
"
```

---