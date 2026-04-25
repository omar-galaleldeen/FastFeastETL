import time
from sqlalchemy import create_engine, text
from utils.logger import get_logger
from config.config_loader import get_config

logger = get_logger(__name__)
_cfg = get_config()


def update_sla_view():
    logger.info("Running scheduled SLA DWH transformation...")

    sla_query = """
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
    """

    try:
        db_config = _cfg.get("database", {})
        conn_str = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )

        engine = create_engine(conn_str)

        with engine.begin() as conn:
            conn.execute(text(sla_query))

        logger.info("SLA view updated successfully")

    except Exception as e:
        logger.error(f"SLA view update failed: {e}")


def sla_scheduler_loop(stop_event):
    """
    Background scheduler loop.
    Stops cleanly when stop_event is set.
    """

    logger.info("SLA scheduler thread started")

    while not stop_event.is_set():
        update_sla_view()

        # sleep in small intervals so shutdown is responsive
        for _ in range(600):  # 10 minutes
            if stop_event.is_set():
                break
            time.sleep(1)

    logger.info("SLA scheduler thread stopped")