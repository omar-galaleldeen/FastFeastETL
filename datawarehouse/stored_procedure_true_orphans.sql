-- ============================================================
-- FastFeast — True Orphan Detection & Promotion
-- ============================================================
-- Purpose:
--   After a 24-hour window, orphan records in the quarantine table
--   that still cannot be resolved against their parent dimension
--   tables are promoted to true_orphan for permanent investigation.
--
-- Background:
--   When an order arrives referencing a customer_id not yet in
--   dim_customer (e.g. customer signed up after batch was generated),
--   the pipeline quarantines it with rejection_stage = 'orphan'.
--   After the next batch loads the missing customer, the record
--   can be retried. If 24 hours pass and the parent still does not
--   exist, the record is a TRUE orphan.
--
-- Contents:
--   1. Helper view: v_pending_orphans
--   2. Function:    promote_true_orphans()
--   3. Manual retry query (re-ingest resolved orphans)
--   4. Monitoring queries
-- ============================================================


-- ============================================================
-- 1. VIEW: pending orphans older than 24 hours
-- ============================================================
CREATE OR REPLACE VIEW v_pending_orphans AS
SELECT
    q.quarantine_id,
    q.source_file,
    q.source_table,
    q.rejection_reason,
    q.raw_data,
    q.quarantined_at,
    NOW() - q.quarantined_at AS age
FROM quarantine q
WHERE q.rejection_stage = 'orphan'
  AND q.quarantined_at < NOW() - INTERVAL '24 hours'
  AND q.quarantine_id NOT IN (
      SELECT quarantine_id FROM true_orphan WHERE quarantine_id IS NOT NULL
  );


-- ============================================================
-- 2. FUNCTION: promote_true_orphans()
-- ============================================================
-- Checks each pending orphan against the current state of
-- dimension tables. If the parent STILL does not exist after
-- 24 hours, promotes to true_orphan.
--
-- Run this on a schedule (e.g. pg_cron daily at 06:30, after
-- batch loads at 06:00).
--
-- Example pg_cron schedule:
--   SELECT cron.schedule('promote-orphans', '30 6 * * *',
--          'SELECT promote_true_orphans()');
-- ============================================================
CREATE OR REPLACE FUNCTION promote_true_orphans()
RETURNS TABLE (
    promoted        INTEGER,
    still_resolvable INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    _promoted         INTEGER := 0;
    _still_resolvable INTEGER := 0;
    _rec              RECORD;
    _parent_exists    BOOLEAN;
    _unresolved_fk    TEXT;
    _fk_value         TEXT;
BEGIN
    -- Loop over every orphan older than 24 hours not yet promoted
    FOR _rec IN SELECT * FROM v_pending_orphans
    LOOP
        _parent_exists := FALSE;
        _unresolved_fk := 'unknown';

        -- ── orders: check customer_id, restaurant_id, driver_id ──────── --
        IF _rec.source_table = 'fact_order' THEN

            IF (_rec.raw_data->>'customer_id') IS NOT NULL THEN
                _fk_value := _rec.raw_data->>'customer_id';
                SELECT EXISTS (
                    SELECT 1 FROM dim_customer
                    WHERE customer_id::TEXT = _fk_value
                ) INTO _parent_exists;
                IF NOT _parent_exists THEN
                    _unresolved_fk := 'customer_id';
                    GOTO do_promote;
                END IF;
            END IF;

            IF (_rec.raw_data->>'restaurant_id') IS NOT NULL THEN
                _fk_value := _rec.raw_data->>'restaurant_id';
                SELECT EXISTS (
                    SELECT 1 FROM dim_restaurant
                    WHERE restaurant_id::TEXT = _fk_value
                ) INTO _parent_exists;
                IF NOT _parent_exists THEN
                    _unresolved_fk := 'restaurant_id';
                    GOTO do_promote;
                END IF;
            END IF;

            IF (_rec.raw_data->>'driver_id') IS NOT NULL THEN
                _fk_value := _rec.raw_data->>'driver_id';
                SELECT EXISTS (
                    SELECT 1 FROM dim_driver
                    WHERE driver_id::TEXT = _fk_value
                ) INTO _parent_exists;
                IF NOT _parent_exists THEN
                    _unresolved_fk := 'driver_id';
                    GOTO do_promote;
                END IF;
            END IF;

            -- All FKs resolved — record is now resolvable, skip promotion
            _still_resolvable := _still_resolvable + 1;
            CONTINUE;

        -- ── tickets: check order_id, agent_id ───────────────────────── --
        ELSIF _rec.source_table = 'fact_ticket' THEN

            IF (_rec.raw_data->>'order_id') IS NOT NULL THEN
                _fk_value := _rec.raw_data->>'order_id';
                SELECT EXISTS (
                    SELECT 1 FROM fact_order
                    WHERE order_id = _fk_value
                ) INTO _parent_exists;
                IF NOT _parent_exists THEN
                    _unresolved_fk := 'order_id';
                    GOTO do_promote;
                END IF;
            END IF;

            IF (_rec.raw_data->>'agent_id') IS NOT NULL THEN
                _fk_value := _rec.raw_data->>'agent_id';
                SELECT EXISTS (
                    SELECT 1 FROM dim_agent
                    WHERE agent_id::TEXT = _fk_value
                ) INTO _parent_exists;
                IF NOT _parent_exists THEN
                    _unresolved_fk := 'agent_id';
                    GOTO do_promote;
                END IF;
            END IF;

            _still_resolvable := _still_resolvable + 1;
            CONTINUE;

        -- ── ticket_events: check ticket_id ──────────────────────────── --
        ELSIF _rec.source_table = 'fact_ticket_event' THEN

            IF (_rec.raw_data->>'ticket_id') IS NOT NULL THEN
                _fk_value := _rec.raw_data->>'ticket_id';
                SELECT EXISTS (
                    SELECT 1 FROM fact_ticket
                    WHERE ticket_id = _fk_value
                ) INTO _parent_exists;
                IF NOT _parent_exists THEN
                    _unresolved_fk := 'ticket_id';
                    GOTO do_promote;
                END IF;
            END IF;

            _still_resolvable := _still_resolvable + 1;
            CONTINUE;

        ELSE
            -- Unknown table — promote conservatively
            _unresolved_fk := 'unknown';
        END IF;

        <<do_promote>>
        INSERT INTO true_orphan (
            quarantine_id, source_file, source_table,
            unresolved_fk, raw_data, first_seen_at
        )
        VALUES (
            _rec.quarantine_id,
            _rec.source_file,
            _rec.source_table,
            _unresolved_fk,
            _rec.raw_data,
            _rec.quarantined_at
        )
        ON CONFLICT DO NOTHING;

        _promoted := _promoted + 1;

    END LOOP;

    RETURN QUERY SELECT _promoted, _still_resolvable;
END;
$$;


-- ============================================================
-- 3. RETRY QUERY
-- ============================================================
-- After a batch loads new customers/drivers, use this query
-- to find orphans that are NOW resolvable (parent arrived).
-- Feed results back through the pipeline for re-ingestion,
-- or INSERT directly if your team prefers.
--
-- Example: orders whose customer_id now exists
-- ============================================================
SELECT
    q.quarantine_id,
    q.source_file,
    q.source_table,
    q.raw_data,
    q.quarantined_at
FROM quarantine q
WHERE q.rejection_stage = 'orphan'
  AND q.source_table    = 'fact_order'
  AND (q.raw_data->>'customer_id')::INTEGER IN (
      SELECT customer_id FROM dim_customer
  )
  AND q.quarantine_id NOT IN (
      SELECT quarantine_id FROM true_orphan WHERE quarantine_id IS NOT NULL
  );


-- ============================================================
-- 4. MONITORING QUERIES
-- ============================================================

-- ── Daily quarantine summary ──────────────────────────────── --
SELECT
    DATE(quarantined_at)    AS quarantine_date,
    source_table,
    rejection_stage,
    COUNT(*)                AS record_count
FROM quarantine
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;


-- ── Orphan rate per file over last 7 days ─────────────────── --
SELECT
    file_name,
    SUM(total_records)                           AS total_ingested,
    SUM(orphan_count)                            AS total_orphans,
    ROUND(
        SUM(orphan_count)::NUMERIC /
        NULLIF(SUM(total_records), 0) * 100, 2
    )                                            AS orphan_rate_pct
FROM pipeline_file_log
WHERE processed_at >= NOW() - INTERVAL '7 days'
GROUP BY file_name
ORDER BY orphan_rate_pct DESC NULLS LAST;


-- ── Files processed today ─────────────────────────────────── --
SELECT
    file_name,
    file_type,
    source_table,
    status,
    total_records,
    valid_records,
    quarantined,
    duplicate_count,
    orphan_count,
    processed_at
FROM pipeline_file_log
WHERE DATE(processed_at) = CURRENT_DATE
ORDER BY processed_at DESC;


-- ── SLA breach summary ────────────────────────────────────── --
SELECT
    DATE(created_at)                                        AS ticket_date,
    COUNT(*)                                                AS total_tickets,
    SUM(CASE WHEN sla_response_breached   THEN 1 ELSE 0 END) AS response_breaches,
    SUM(CASE WHEN sla_resolution_breached THEN 1 ELSE 0 END) AS resolution_breaches,
    ROUND(
        SUM(CASE WHEN sla_response_breached THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                       AS response_breach_pct,
    ROUND(
        SUM(CASE WHEN sla_resolution_breached THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                       AS resolution_breach_pct,
    ROUND(AVG(first_response_min), 2)                       AS avg_first_response_min,
    ROUND(AVG(resolution_min), 2)                           AS avg_resolution_min
FROM fact_ticket
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY ticket_date DESC;


-- ── True orphan summary ───────────────────────────────────── --
SELECT
    source_table,
    unresolved_fk,
    COUNT(*)                         AS orphan_count,
    MIN(first_seen_at)               AS oldest,
    MAX(first_seen_at)               AS newest,
    AVG(retry_count)                 AS avg_retries
FROM true_orphan
GROUP BY source_table, unresolved_fk
ORDER BY orphan_count DESC;
