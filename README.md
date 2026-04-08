# FastFeast ETL Pipeline

A production-grade, near real-time data pipeline built for **FastFeast** — a rapidly growing food delivery platform. The pipeline extracts batch and micro-batch data from OLTP exports, validates and transforms it, loads it into a PostgreSQL dimensional model, runs OLAP analytics, efficiently processing both batch and micro-batch operational data. It extracts data from OLTP exports, performs comprehensive validation and transformation, loads the refined data into a PostgreSQL-based dimensional model, executes OLAP analytics, and ultimately feeds a Power BI dashboard for insightful visualization.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Pipeline Phases](#pipeline-phases)
- [Data Flow](#data-flow)
- [Threading Model](#threading-model)
- [Configuration](#configuration)
- [Data Generation](#data-generation)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Testing](#testing)
- [Logging](#logging)
- [Tech Stack](#tech-stack)

---

## Overview

FastFeast exports operational data in two modes:

| Mode | Frequency | Files |
|---|---|---|
| **Batch** | Once daily | 13 dimension files (CSV + JSON) |
| **Micro-batch** | Continuously throughout the day | 3 fact files (CSV + JSON) |

The pipeline handles both modes simultaneously using dedicated daemon threads, processes files through a multi-phase ETL flow, and guarantees that the pipeline **never stops** — all errors are caught, logged, and skipped while processing continues.

---

## Architecture

```
FastFeast OLTP exports
        │
        ▼
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
                       │ validated DataFrames
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   transformation/                        │
│  dim_transformer · fact_transformer · sla_calculator    │
└──────────────────────┬──────────────────────────────────┘
                       │ transformed DataFrames
                       ▼
┌─────────────────────────────────────────────────────────┐
│                     loading/                             │
│  dim_loader · fact_loader · db_connector (PostgreSQL)   │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                    warehouse/                            │
│  schema_builder · analytics_builder · date_dim_builder  │
│  SLA metrics · revenue impact · ticket summaries        │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
              Power BI Dashboard
```

---

## Project Structure

```
FastFeastETL/
│
├── main.py                         # single entry point
│
├── config/
│   ├── config_loader.py            # loads + caches config once
│   └── pipeline_config.yaml        # all settings — no hardcoding
│
├── ingestion/
│   ├── __init__.py
│   ├── ingestion_runner.py         # orchestrates ingestion phase
│   ├── file_watcher.py             # batch + stream file discovery
│   ├── file_tracker.py             # SQLite idempotency tracking
│   └── file_reader.py              # CSV / JSON → DataFrame
│
├── validation/
│   ├── __init__.py
│   ├── schema_validator.py         # required columns + types
│   ├── record_validator.py         # nulls, email, phone, dates, ranges
│   ├── duplicate_detector.py       # primary key deduplication
│   ├── pii_handler.py              # hash / drop PII fields
│   ├── orphan_checker.py           # FK validation + orphan rate
│   ├── quarantine.py               # rejected record storage
│   └── validation_runner.py        # orchestrates validation phase
│
├── transformation/
│   ├── __init__.py
│   ├── dim_transformer.py          # normalize, cast, surrogate keys
│   ├── fact_transformer.py         # enrich orders, tickets, events
│   ├── sla_calculator.py           # response time, breach flag, reopen
│   └── transformation_runner.py    # orchestrates transformation phase
│
├── loading/
│   ├── __init__.py
│   ├── dim_loader.py               # upsert dimensions
│   ├── fact_loader.py              # insert facts
│   ├── db_connector.py             # PostgreSQL connection
│   └── loader_runner.py            # orchestrates loading phase
│
├── warehouse/
│   ├── __init__.py
│   ├── schema_builder.py           # CREATE dim + fact tables
│   ├── date_dim_builder.py         # date dimension
│   ├── analytics_builder.py        # SLA, revenue, quality views
│   └── dwh_manager.py              # init schema + run OLAP calcs
│
├── monitoring/
│   ├── __init__.py
│   ├── logger.py                   # JSON structured logging (QueueListener)
│   ├── log_formatter.py            # step-level log format
│   ├── log_rotator.py              # auto-delete logs older than N days
│   ├── metrics_collector.py        # quality metrics accumulator
│   └── alerter.py                  # async email alerts on failure only
│
├── reporting/
│   ├── __init__.py
│   ├── pdf_builder.py              # daily quality PDF report
│   ├── report_emailer.py           # attach + send PDF by email
│   └── report_runner.py            # orchestrates daily report
│
├── scripts/
│   ├── generate_master_data.py     # creates base OLTP tables (once)
│   ├── generate_batch_data.py      # daily dimension snapshots
│   ├── generate_stream_data.py     # hourly transaction files
│   ├── add_new_customers.py        # simulate new signups
│   └── add_new_drivers.py          # simulate new drivers
│
├── utils/
│   └── logger.py
│
├── data/
│   ├── input/
│   │   ├── batch/YYYY-MM-DD/       # dimension files (flat folder)
│   │   └── stream/YYYY-MM-DD/HH/  # fact files (hourly subfolders)
│   ├── tracker/                    # SQLite idempotency database
│   └── quarantine/                 # rejected records
│
└── logs/
    └── pipeline_YYYY-MM-DD.log     # one JSON log file per day
```

---

## Pipeline Phases

### Phase 1 — Ingestion
Discovers files from `data/input/batch/` and `data/input/stream/` using two dedicated daemon threads. Reads CSV and JSON files into raw DataFrames. Tracks every processed file by MD5 hash in SQLite to guarantee idempotency — re-running the pipeline never duplicates data.

### Phase 2 — Validation
Validates every record against schema rules, business rules, and referential integrity. Rejects invalid records to quarantine without stopping the pipeline. Detects duplicates, validates email/phone formats, checks numeric ranges, and masks PII before it reaches the analytics layer.

### Phase 3 — Transformation
Normalizes dimension data, assigns surrogate keys, enriches fact records with date keys, and calculates SLA metrics — first response time, resolution time, breach flags, and reopen rates — entirely in the OLAP layer.

### Phase 4 — Loading
Upserts dimension records (insert new, update changed) and inserts fact records into PostgreSQL. All writes are idempotent — `ON CONFLICT DO UPDATE` prevents duplicates at the database level.

### Phase 5 — Warehouse / OLAP
Builds and maintains the dimensional model in PostgreSQL. Creates analytical views for SLA monitoring, revenue impact, complaint rates, and data quality metrics.

---

## Data Flow

**Batch files** (13 dimension files, daily):
```
data/input/batch/2026-02-20/
├── customers.csv     → Dim_Customer
├── drivers.csv       → Dim_Driver
├── agents.csv        → Dim_Agent
├── restaurants.json  → Dim_Restaurant
├── cities.json       → Dim_City
└── ...               → ...
```

**Stream files** (3 fact files, continuously):
```
data/input/stream/2026-02-20/09/
├── orders.json        → Fact_Order
├── tickets.csv        → Fact_Ticket
└── ticket_events.json → Fact_Ticket_Event
```

---

## Threading Model

The pipeline uses dedicated daemon threads to keep every I/O operation off the main processing loop:

| Thread | Purpose |
|---|---|
| `BatchWatcherThread` | Polls for batch files once daily at `trigger_hour` |
| `StreamWatcherThread` | Watchdog OS-level observer — fires instantly on new stream files |
| `_TrackerThread` | Only thread that touches SQLite — all tracker I/O isolated here |
| `QueueListener` | Only thread that writes to the log file — no log I/O on pipeline threads |
| `AlerterThread` | Spawned only on failure — sends email without blocking the pipeline |

All threads communicate through thread-safe `Queue` objects. The pipeline main loop never blocks on disk I/O.

---

## Configuration

All settings live in `config/pipeline_config.yaml` — nothing is hardcoded:

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
```

---

## Data Generation

Simulate FastFeast's OLTP exports using the provided scripts:

```bash
# 1. create base OLTP source tables (run once at project start)
python scripts/generate_master_data.py

# 2. create daily dimension snapshots
python scripts/generate_batch_data.py --date 2026-02-20

# 3. simulate new customer signups (optional)
python scripts/add_new_customers.py --count 5

# 4. simulate new driver onboarding (optional)
python scripts/add_new_drivers.py --count 3

# 5. generate hourly stream files throughout the day
python scripts/generate_stream_data.py --date 2026-02-20 --hour 9
python scripts/generate_stream_data.py --date 2026-02-20 --hour 14
```

---

## Getting Started

**1. Clone the repository**
```bash
git clone https://github.com/omar-galaleldeen/FastFeastETL.git
cd FastFeastETL
```

**2. Create a virtual environment**
```bash
python -m venv venv
source venv/bin/activate        # Linux / Mac
venv\Scripts\activate           # Windows
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Generate test data**
```bash
python scripts/generate_master_data.py
python scripts/generate_batch_data.py --date 2026-02-20
python scripts/generate_stream_data.py --date 2026-02-20 --hour 9
```

**5. Run the pipeline**
```bash
python main.py
```

**6. Generate more streaming data**
```bash
python scripts/generate_stream_data.py --date 2026-02-20 --hour 9 10 11 12
```

---

## Running the Pipeline

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

---

## Testing

Individual component tests are provided to verify each module in isolation:

```bash
# test batch file discovery
python test_watcher_batch.py

# test stream file detection (watchdog)
python test_watcher_stream.py

# test tracker idempotency on real data
python test_tracker_real.py
```

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
