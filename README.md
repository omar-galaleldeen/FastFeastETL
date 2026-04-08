# FastFeastETL

> **A production-grade, near real-time data pipeline built for **FastFeast** — a rapidly growing food delivery platform. The pipeline extracts batch and micro-batch data from OLTP exports, validates and transforms it, loads it into a PostgreSQL dimensional model, runs OLAP analytics, efficiently processing both batch and micro-batch operational data. It extracts data from OLTP exports, performs comprehensive validation and transformation, loads the refined data into a PostgreSQL-based dimensional model, executes OLAP analytics, and ultimately feeds a Power BI dashboard for insightful visualization.**
> 
---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Features](#features)
  - [Threading Model](#threading-model)
  - [Dual-Mode Data Ingestion](#dual-mode-data-ingestion)
- [Configuration](#configuration)
- [Data Generation](#data-generation)
- [Getting Started](#getting-started)
- [Orphan Handling](#orphan-handling)
- [Logging](#logging)
- [Tech Stack](#tech-stack)

---

## Overview

FastFeastETL is a comprehensive ETL system designed for processing food delivery platform data at scale. It ingests both batch (daily snapshots) and stream (real-time events) data, validates against schema and business rules, detects orphaned records, loads into a dimensional PostgreSQL warehouse, and provides analytics-ready data for business intelligence.

**Core Capabilities:**
- **Dual-Mode Ingestion** - Batch (CSV) and stream (JSON) data sources
- **Schema-Driven Validation** - Type checking, nullable constraints, business rules
- **Foreign Key Integrity** - Orphan detection with configurable 24-hour resolution window
- **Quarantine System** - Rejected records stored for analysis and recovery
- **Dimensional Warehouse** - Star schema optimized for analytics (facts + dimensions)
- **Thread-Safe Operations** - Concurrent processing via psycopg2 connection pooling
- **Data Seeding** - Master data initialization and daily snapshot generation
- **Comprehensive Logging** - File logs, pipeline metadata, and audit trails

---

## Architecture

```
                                    BATCH DATA                            STREAM DATA
                                (Daily CSV snapshots)               (Real-time JSON events)
                                        │                                      │
                                        ▼                                      ▼
                                ┌─────────────────────────────────────────────────────────┐
                                │                     ingestion/                           │
                                │  BatchWatcherThread ──┐                                  │
                                │                        ├──▶ Queue[Path] ──▶ runner loop │
                                │  StreamWatcherThread ─┘                                  │
                                │  (watchdog — OS inotify)    tracker · reader             │
                                └──────────────────────┬──────────────────────────────────┘
                                                       │ clean DataFrames
                                                       ▼
                                ┌─────────────────────────────────────────────────────────┐
                                │                    validation/                           │
                                │  schema · nulls · formats · duplicates · orphans · PII  │
                                └──────────────────────┬──────────────────────────────────┘
                                                       │ 
                                                       │
                                                ┌──────┴──────┐
                                                ↓             ↓
                                              [PASS]       [FAIL]
                                                ↓             ↓
                                                │      ┌──────────────┐
                                                │      │  QUARANTINE  │
                                                │      │ (bad records)│
                                                │      └──────────────┘
                                                │ 
                                                │  
                                                ↓  validated DataFrames
                                                ┌──────────────────────────┐
                                                │   FK CHECKER / ORPHAN    │
                                                │   VALIDATOR              │
                                                │  (check refs vs DWH)     │
                                                └────────┬─────────────────┘
                                                         │
                                                  ┌──────┴─────────┐
                                                  ↓                ↓
                                               [CLEAN]        [ORPHAN]
                                                  ↓                ↓
                                                  │         ┌─────────────────┐
                                                  │         │ ORPHAN QUEUE    │
                                                  │         │ (24h retry loop)│
                                                  │         └─────────────────┘
                                                  ↓
                                                  ┌─────────────────────────┐
                                                  │  DIMENSION LOADER       │
                                                  │  (INSERT w/ idempotency)│
                                                  └────────┬────────────────┘
                                                           ↓
                                                  ┌─────────────────────────┐
                                                  │  FACT LOADER            │
                                                  │  (SLA calcs, date keys) │
                                                  └────────┬────────────────┘
                                                           ↓
                                                    PostgreSQL DWH
                                                    ├─ dim_customer
                                                    ├─ dim_driver
                                                    ├─ dim_restaurant
                                                    ├─ dim_agent
                                                    ├─ dim_region
                                                    ├─ fact_order
                                                    ├─ fact_ticket
                                                    └─ fact_ticket_event
```

---

## Project Structure

```
FastFeastETL/
├── datawarehouse/                             # PostgreSQL warehouse layer
│   ├── db_connection.py                       # Thread-safe connection pool
│   ├── dim_loader.py                          # Dimension table INSERT
│   ├── fact_loader.py                         # Fact table INSERT + SLA calcs
│   ├── file_log.py                            # Pipeline audit trail
│   ├── fk_checker.py                          # FK validation vs existing PKs
│   ├── loading_into_DW.py                     # Loading data into DW
│   └── master_seeder.py                       # Seed master data at startup
│   └── quarantine_loader.py                   # Rejected record storage
│   ├── schema_init.py                         # Create Postgres schema
|   ├── schema_ddl.sql                         # Postgres DDL (dims + facts)
|   ├── schema_ddl_old_checkconstraint.sql  
|   ├── stored_procedure_true_orphans.sql      # Orphan promotion logic
│
├── ingestion/                                 # Source data handling
│   ├── file_reader.py                         # CSV/JSON parsing
│   ├── file_tracker.py                        # Idempotency via MD5 hashing
│   ├── file_watcher.py                        # Watcher threads for batch , stream
│   └── ingestion_runner.py                    # Orchestration & watchers
│
├── validation/                                # Data quality layer
│   ├── schema_registry.py                     # Source schema definitions
|   ├── schema_validator.py                    # Type coercion & nullability
│   ├── batch_records_validator.py             # Batch record-level checks
|   ├── stream_records_validator.py            # Stream record-level checks
|   ├── orphan_validator.py                    # FK orphan detection
│   ├── pii_handler.py                         # Hash PII fields
│   ├── fault_handler.py                       # Reject record dispatch
│   └── validation_runner.py                   # Validation orchestration
│
├── scripts/                                   # Data generators & utilities
│   ├── generate_master_data.py                # Initialize dimensions
│   ├── generate_batch_data.py                 # Daily batch snapshots
│   ├── generate_stream_data.py                # Hourly stream events
│   ├── add_new_customers.py                   # Add customers (orphan test)
│   ├── add_new_drivers.py                     # Add drivers (orphan test)
│   ├── simulate_day.py                        # Full day simulation
│
├── config/                                    # Configuration files
│   ├── config_loader.py                       # YAML config loading
│   ├── pipeline_config.yaml                   # File types, expected files, FK rules
│
├── data/
│   ├── master/                                # Base dimensions (CSV)
│   │   ├── regions.csv
│   │   ├── categories.csv
│   │   ├── customers.csv
│   │   ├── drivers.csv
│   │   ├── restaurants.csv
│   │   ├── agents.csv
│   │   ├── segments.csv
│   │   ├── teams.csv
│   │   ├── channels.csv
│   │   ├── priorities.csv
│   │   ├── reasons.csv
│   │   ├── reason_categories.csv
│   │   ├── cities.csv
│   │   └── metadata.json
│   │
│   ├── quarantine/                            # Rejected records (CSV)
|   |
│   ├── input/
│   │   ├── batch/                             # Daily batch snapshots
│   │   │   └── YYYY-MM-DD/
│   │   └── stream/                            # Hourly stream events
│   │       └── YYYY-MM-DD/HH/
│   │
│   └── tracker/
│       ├── pipeline_tracker.db                 # SQLite idempotency DB

│
├── logs/                                       # Application logs
│   ├── pipeline_YYYY-MM-DD.log
│
├── utils/                                      # Shared utilities
│   ├── logger.py                               # Logging config
│   ├── alerter.py                              # Alert notifications
│   ├── sla_updater_job.py                      # calculating SLA metrics from fact_ticket

│
├── main.py                                     # Application entry point
└── README.md                                   # This file
└── README_datawarehouse.md     
```

---

## Features

### Threading Model

The pipeline uses dedicated daemon threads to keep every I/O operation off the main processing loop:

| Thread | Purpose |
|---|---|
| `BatchWatcherThread` | Polls for batch files once daily at `trigger_hour` |
| `StreamWatcherThread` | Watchdog OS-level observer — fires instantly on new stream files |
| `_TrackerThread` | Only thread that touches SQLite — all tracker I/O isolated here |
| `QueueListener` | Only thread that writes to the log file — no log I/O on pipeline threads |
| `AlerterThread` | Spawned only on failure — sends email without blocking the pipeline |

All threads communicate through thread-safe `Queue` objects. The pipeline main loop never blocks on disk I/O.


### Dual-Mode Data Ingestion

**Batch Processing**
- Daily CSV, JSON snapshots from `data/input/batch/{YYYY-MM-DD}/`
- Scheduled ingestion with file-based watchers
- Supports: customers, drivers, agents, restaurants, regions, categories, etc.
- Idempotency via MD5 hash tracking in SQLite

**Stream Processing**
- Real-time CSV, JSON events from `data/input/stream/{YYYY-MM-DD}/{HH}/`
- Hourly file watchers for continuous data flow
- Supports: orders, tickets, ticket_events
- Orphan queuing for unresolved foreign keys


### ✅ Schema Validation & Data Quality

- **Type Coercion** - Automatic casting (int, float, datetime, boolean)
- **Nullability Checks** - Enforce required vs. optional columns
- **Business Rules** - Custom validation constraints per data type
- **PII Masking** - Hash sensitive fields (email, phone)
- **Rejection Handling** - Quarantine invalid records for manual review

### 🔗 Foreign Key Integrity & Orphan Detection

- **Real-time FK Checks** - Validate references against existing PKs
- **Orphan Queuing** - Quarantine records with unresolved FKs
- **24-Hour Grace Period** - Allow time for parent records to arrive
- **True Orphan Detection** - SQL stored procedure promotes unrecoverable orphans
- **Retry Mechanism** - Re-ingest orphans once parent arrives

  
---

### 💾 PostgreSQL Dimensional Warehouse

**Dimension Tables**
- `dim_customer` - Customer master data
- `dim_driver` - Driver master data
- `dim_restaurant` - Restaurant master data
- `dim_agent` - Customer service agent master data
- `dim_region`, `dim_category`, `dim_segment`, `dim_team`, `dim_channel`, `dim_priority`, `dim_reason`

**Fact Tables**
- `fact_order` - Order transactions with SLA tracking
- `fact_ticket` - Customer support tickets with SLA breach flags
- `fact_ticket_event` - Ticket status transitions and activities

**Analytics Tables**
- `pipeline_file_log` - Audit trail of every file processed
- `quarantine` - Rejected records (JSONB format)
- `true_orphan` - Permanently unresolved orphans


---

## Configuration

All settings live in `config/pipeline_config.yaml` — nothing is hardcoded, defines file types, watchers, validation rules, and FK relationships.:

```yaml
watcher:
  batch:
    dir: "data/input/batch"
    trigger_hour: 6
    expected_files:
      customers.csv:
        table: "Dim_Customer"
      # ... all 13 batch files

  stream:
    dir: "data/input/stream"
    expected_files:
      orders.json:
        table: "Fact_Order"
      # ... all 3 stream files

tracker:
  db_path: "data/tracker/pipeline_tracker.db"

logging:
  dir: "logs"
  retention_days: 7

alerting:
  enabled: True
  smtp:
    host: "smtp.gmail.com"
    port: 587
    user_name: "your_email@gmail.com"
    password: "YOUR_APP_PASSWORD_HERE"
    sender: "your_email@gmail.com"
    receivers: 
      - "your_email@gmail.com"

schemas:
  customers:
    primary_key: customer_id
    columns:
      - { name: region_id,         dtype: int,   nullable: false }

  # ... all tables
```

---

## Data Generation

Simulate FastFeast's OLTP exports using the provided scripts:

```bash
# 1. create base OLTP source tables (run once at project start)
python scripts/generate_master_data.py

# 2. create daily dimension snapshots
python scripts/generate_batch_data.py --date 2026-02-20

# 5. generate hourly stream files throughout the day
python scripts/generate_stream_data.py --date 2026-02-20 --hour 9
python scripts/generate_stream_data.py --date 2026-02-20 --hour 14
```

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/omar-galaleldeen/FastFeastETL.git
cd FastFeastETL
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```


### 3. Generate Test Data

```bash
python scripts/generate_master_data.py
python scripts/generate_batch_data.py --date 2026-02-20
python scripts/generate_stream_data.py --date 2026-02-20 --hour 9
```

### 4. Start the Pipeline

```bash
python main.py
```

The pipeline starts and runs continuously. Stop it cleanly with `Ctrl+C` — all threads shut down gracefully and remaining logs are flushed before exit.

Expected startup logs:

```json
{"time": "2026-02-20 06:00:00", "level": "INFO", "module": "main", "msg": "[main] FastFeast ETL pipeline starting"}
{"time": "2026-02-20 06:00:00", "level": "INFO", "module": "file_watcher", "msg": "[batch] watcher started"}
{"time": "2026-02-20 06:00:00", "level": "INFO", "module": "file_watcher", "msg": "[stream] observer started"}
{"time": "2026-02-20 06:00:00", "level": "INFO", "module": "ingestion_runner", "msg": "[runner] loop started"}
```

### 5. Generate more streaming data
```bash
python scripts/generate_stream_data.py --date 2026-02-20 --hour 9 10 11 12
```

---

### Monitoring

**Check Pipeline Logs**
```bash
tail -f logs/pipeline_YYYY-MM-DD.log
```

**Query Pipeline File Log**
```sql
SELECT file_name, status, total_records, valid_records, quarantined 
FROM pipeline_file_log 
ORDER BY processed_at DESC LIMIT 20;
```

**Check Quarantine**
```sql
SELECT source_table, rejection_stage, COUNT(*) 
FROM quarantine 
GROUP BY source_table, rejection_stage;
```

**Monitor Orphans**
```sql
SELECT source_table, COUNT(*) 
FROM quarantine 
WHERE rejection_stage = 'orphan' 
GROUP BY source_table;
```

---


## Orphan Handling

FastFeastETL implements a sophisticated orphan resolution strategy:

### Detection
1. **Ingestion** - Stream orders arrive referencing customer_id=999
2. **FK Check** - fk_checker queries dim_customer, finds no match
3. **Quarantine** - Record marked as 'orphan' in quarantine table

### Resolution Window (24 hours)
1. **Next Batch** - Batch data loads the missing customer (customer_id=999)
2. **Retry Promotion** - SQL query `v_pending_orphans` finds resolvable records
3. **Re-ingest** - Orphan record is pulled from quarantine and reprocessed

### Permanent Orphans
1. **24-Hour Check** - `promote_true_orphans()` stored procedure runs
2. **Final Validation** - Checks if parent STILL doesn't exist
3. **True Orphan** - Record moved to `true_orphan` table for investigation

---

## Logging

All logs are written as structured JSON — one line per event — to `logs/pipeline_YYYY-MM-DD.log`. A new file is created each day and files older than 7 days are deleted automatically.

Example log lines:

```json
{"time": "2026-02-20 06:00:01", "level": "INFO",  "module": "ingestion_runner", "msg": "[step:read]    done    | file=customers.csv | type=customers | rows=1500 | cols=8"}
{"time": "2026-02-20 06:00:01", "level": "INFO",  "module": "ingestion_runner", "msg": "[step:load]    done    | file=customers.csv | records=1500"}
{"time": "2026-02-20 06:00:02", "level": "ERROR", "module": "ingestion_runner", "msg": "[step:read]    failed  | file=orders.json | error=malformed JSON on line 42"}
```

Alerts are sent by email **only on failure** — never on successful processing.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.10+ |
| File watching | `watchdog` |
| Data processing | `pandas` |
| Data warehouse | PostgreSQL |
| Analytics | pandas (OLAP layer) |
| Logging | Python `logging` + `QueueListener` |
| Configuration | YAML (`pyyaml`) |
| Idempotency tracking | SQLite (`sqlite3`) |
| Threading | Python `threading` + `concurrent.futures` |

---


