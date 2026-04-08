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
        CASE WHEN first_response_at <= sla_first_due_at THEN True ELSE False END AS is_first_response_met,
        CASE WHEN resolved_at <= sla_resolve_due_at THEN True ELSE False END AS is_resolution_met
    FROM 
        tickets;
    """
    
    try:
        db_config = _cfg.get("database", {})
        conn_str = f"postgresql://{db_config.get('user', 'postgres')}:{db_config.get('password', 'admin')}@{db_config.get('host', 'localhost')}:{db_config.get('port', '5432')}/{db_config.get('dbname', 'fastfeast_dwh')}"
        engine = create_engine(conn_str)
        
        with engine.begin() as conn:
            conn.execute(text(sla_query))
        logger.info("✅ SLA View updated successfully!")
    except Exception as e:
        logger.error(f"❌ Failed to update SLA View: {e}")

if __name__ == "__main__":
    while True:
        update_sla_view()
        time.sleep(300)