# FastFeast — Datawarehouse Module

## Structure

```
datawarehouse/
├── __init__.py
├── db_connection.py       # Connection pool + database creation
├── schema_init.py         # CREATE TABLE DDL (Python, runs on startup)
├── fk_checker.py          # FK lookups against Postgres (replaces SQLite reference.db)
├── dim_loader.py          # INSERT for dimension tables
├── fact_loader.py         # INSERT for fact tables + SLA calculation
├── quarantine_loader.py   # Writes rejected records to quarantine table
└── file_log.py            # Writes per-file processing record to pipeline_file_log
```

---

## Configuration

Add to `config/pipeline_config.yaml`:

```yaml
database:
  host:     localhost
  port:     5432
  user:     postgres
  password: "your_password"
  dbname:   fastfeastapp
```

The password can also be set via the `PGPASSWORD` environment variable.

---

## Startup sequence

`ingestion_runner.start()` calls these in order:

```
init_pool()      ← creates fastfeastapp DB if missing, opens connection pool
init_schema()    ← runs all CREATE TABLE IF NOT EXISTS statements
file_tracker.start()
file_watcher.start()
_run_loop()      ← blocks forever, processes files as they arrive
```

---

## Per-file pipeline flow

```
File arrives (watchdog / batch scan)
    │
    ▼
hash → idempotency check (SQLite file_tracker)
    │
    ▼
read_file() → raw DataFrame
    │
    ▼
validation_runner.run()
    ├── schema_validator        → quarantine (CSV + Postgres) on failure
    ├── batch/stream_validator  → quarantine on failure
    │       duplicate_count tracked and logged
    └── orphan_validator        → fk_checker queries Postgres dims
            → quarantine on failure (rejection_stage = 'orphan')
    │
    ▼  clean DataFrame (PII masked)
    │
    ▼
load_dimension(file_name, df)   ← batch files
    OR
load_fact(file_name, df)        ← stream files
    │
    ▼
file_log.record()   ← pipeline_file_log row in Postgres
file_tracker.mark_as_done()  ← SQLite idempotency record
```

---

## Design decisions

### No CHECK constraints / No FK constraints on facts
All business rule validation (ranges, formats, referential integrity) is
done by the validation layer before data reaches Postgres. Adding database
constraints would create a second validation layer that fights with the
pipeline and degrades INSERT performance.

### Pure INSERT (no upsert)
By the time a row reaches a loader it has passed deduplication. Using
`ON CONFLICT DO NOTHING` as a safety net only — not as the primary
dedup mechanism.

### psycopg2 executemany (not SQLAlchemy to_sql)
`execute_batch` with `page_size=500` is significantly faster than
SQLAlchemy's `to_sql` for bulk inserts. No ORM overhead.

### ThreadedConnectionPool (min=2, max=10)
The pipeline processes one file at a time from a single thread queue.
The pool is sized generously in case you add parallel file processing later.

### SQLite file_tracker kept for idempotency
The SQLite tracker is purpose-built for fast hash-based dedup. It stays.
`pipeline_file_log` in Postgres is the analytics-facing record — it does
not replace the tracker, it complements it.

---

## Quarantine table

```sql
SELECT * FROM quarantine
WHERE rejection_stage = 'orphan'     -- FK failed
  AND source_table    = 'fact_order'
  AND quarantined_at  > NOW() - INTERVAL '1 hour';
```

`raw_data` is JSONB — query any field:
```sql
SELECT raw_data->>'customer_id', COUNT(*)
FROM quarantine
WHERE rejection_stage = 'orphan'
GROUP BY 1;
```

---

## True orphan promotion

Run `stored_procedure_true_orphans.sql` in psql to create the function:

```bash
psql -U postgres -d fastfeastapp -f stored_procedure_true_orphans.sql
```

Then call it manually or schedule with pg_cron:

```sql
-- Manual
SELECT * FROM promote_true_orphans();

-- pg_cron (runs daily at 06:30, after batch loads at 06:00)
SELECT cron.schedule(
    'promote-orphans',
    '30 6 * * *',
    'SELECT promote_true_orphans()'
);
```

---

## Monitoring

All queries in `stored_procedure_true_orphans.sql` under section 4.
Key ones:

```sql
-- Files processed today
SELECT file_name, status, total_records, valid_records, quarantined
FROM pipeline_file_log
WHERE DATE(processed_at) = CURRENT_DATE
ORDER BY processed_at DESC;

-- SLA breach rate last 30 days
SELECT DATE(created_at), response_breach_pct, resolution_breach_pct
FROM (... see full query in stored_procedure_true_orphans.sql)

-- Orphan rate by file
SELECT file_name, orphan_rate_pct
FROM pipeline_file_log
GROUP BY file_name
ORDER BY orphan_rate_pct DESC;
```

---

## FK reference map

`fk_checker.FK_TABLE_MAP` maps config names to Postgres tables:

| Config name  | Postgres table    | PK column       |
|---|---|---|
| customers    | dim_customer      | customer_id     |
| drivers      | dim_driver        | driver_id       |
| restaurants  | dim_restaurant    | restaurant_id   |
| agents       | dim_agent         | agent_id        |
| orders       | fact_order        | order_id        |
| tickets      | fact_ticket       | ticket_id       |

To add a new FK reference, add one entry to `FK_TABLE_MAP` in
`fk_checker.py` and add the FK rule to `pipeline_config.yaml`
under the relevant stream file's `foreign_keys:` section.

---

## Adding a new dimension table

1. Add DDL to `schema_init.py` `_DDL` list
2. Add loader function to `dim_loader.py`
3. Register in `dim_loader.DIM_LOADERS` dict
4. Add entry to `_FILE_TO_TABLE` in `ingestion_runner.py`
5. Add schema to `pipeline_config.yaml` `schemas:` section
6. Add to `file_type_map` and `watcher.batch.expected_files`

No other files need changes.
