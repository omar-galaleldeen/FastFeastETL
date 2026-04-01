import pandas as pd
from sqlalchemy import create_engine
from utils.logger import get_logger
from config.config_loader import get_config

logger = get_logger(__name__)
_cfg = get_config()

def load_to_postgres(df: pd.DataFrame, table_name: str, processed_at: str):
    """
    Loads a clean Pandas DataFrame into a PostgreSQL table.
    """
    if df is None or df.empty:
        return

    logger.info(f"Preparing to load {len(df)} records into '{table_name}'...")

    try:
        final_df = df.copy()
        final_df['etl_processed_at'] = processed_at
        db_config = _cfg.get("database", {})
        user     = db_config.get("user", "postgres")
        password = db_config.get("password", "admin")
        host     = db_config.get("host", "localhost")
        port     = db_config.get("port", "5432")
        dbname   = db_config.get("dbname", "fastfeast")

        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(conn_str)

        final_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi', 
            chunksize=1000
        )

        logger.info(f"Successfully loaded {len(final_df)} records into '{table_name}'.")

    except Exception as e:
        logger.error(f"Failed to load data into Postgres table '{table_name}': {e}")
