import sqlite3

import pandas as pd
import os
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)

class orphan_validator:

    def __init__(self , valid_df , file_name):
            self.df = valid_df
            self.file_name = file_name

    def run(self):
        """
        Run referential validation (orphan checking) for stream files.
        """

        is_orphan_valid, valid_df , orphan_df = self.check_orphans(self.df , self.file_name)
        if not is_orphan_valid:
            print(f"Found {orphan_df.shape[0]} Orphans in {self.file_name} & moving to quarantine")
            #self.track_orphans_for_quality(orphan_df)
            print(orphan_df.to_string())
            print(f'Valid Rows: {valid_df.shape[0]}, Quarantined Rows: {orphan_df.shape[0]}') 
            #return False, valid_df, orphan_df

        reference_tables = ['orders' , 'tickets']
        if self.file_name in reference_tables:
            self.update_reference_table(valid_df)

        #return True , valid_df , None

    def update_reference_table(self, df):
        """
        Update the reference table in SQLite with validated IDs for FK relationships.

        Extracts the first column from the validated DataFrame  and inserts its values
        into a corresponding table in the 'reference/reference.db' SQLite database.
        """
        try:
            if df is None or df.empty:
                return
            
            table_name = self.file_name
            os.makedirs('reference', exist_ok=True)

            db_path = "reference/reference.db"
            first_col = df.iloc[:, 0]

            
            with sqlite3.connect(db_path) as conn:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id TEXT PRIMARY KEY
                    )
                """)
                
                conn.executemany(
                    f"INSERT OR IGNORE INTO {table_name} (id) VALUES (?)",
                    [(id,) for id in first_col]
                )
                
        except Exception as e:
            logger.error(f"Error updating reference file: {e}")

 
    def check_orphans(self , df , file_name):
        """
        Check for orphaned records (child records with missing parents) using
        SQLite reference tables.
        """
        fk_rules = self._get_foreign_key_rules(file_name)

        if not fk_rules:
            return True, df, None
    
        orphan_df = pd.DataFrame()
        db_path = 'reference/reference.db'

        for col , ref_table in fk_rules.items():
            try:
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (ref_table,)
                    )

                    if not cursor.fetchone():
                        logger.warning(f"Reference table {ref_table} not found in DB, skipping FK check for {col}")
                        orphans = df.copy()

                    else:
                        ref_ids = pd.read_sql_query(f"SELECT id FROM {ref_table}", conn)
                        # convert to set for fast lookup
                        valid_ids = set(ref_ids['id'].astype(str))
                        orphans = df[~ df[col].astype(str).isin(valid_ids)]

                if not orphans.empty:
                    orphan_df = pd.concat([orphan_df , orphans])
                    logger.error(f"Found {orphans.shape[0]} orphans in {file_name} in {col}")
                    #move_to_quarantine(orphan_df, "didn't pass referential integrity" , f"{self.file_name}")

            except Exception as e:
                logger.error(f"Error reading reference table {ref_table}: {e}")
                continue


        if not orphan_df.empty:
            orphan_df = orphan_df.drop_duplicates()
            valid_df = df.drop(orphan_df.index)
            return False, valid_df , orphan_df

        return True, df , None
    

    def _get_foreign_key_rules(self, file_name):
        """
        Get FK rules for the file from config
        """
        try:
            _cfg = get_config()
            STREAM_FILES = _cfg.get("watcher", {}).get("stream", {}).get("expected_files", {})

            file_config = next(
                (cfg for fname, cfg in STREAM_FILES.items()
                    if os.path.splitext(fname)[0] == file_name),
                    {}
            )
            
            return file_config.get("foreign_keys", {})
            
        except Exception as e:
            logger.error(f"Error loading FK rules: {e}")
            return {}
    
    # def track_orphans_for_quality(self, orphan_df):
    #     """
    #     Track orphans for quality metrics (don't remove)
    #     """
    #     try:
    #         orphan_stats = {
    #             'file_name': self.file_name,
    #             'orphan_count': orphan_df.shape[0],
    #             'orphan_rate': (orphan_df.shape[0] / self.df.shape[0] * 100) if self.df.shape[0] > 0 else 0,
    #             'timestamp': pd.Timestamp.now()
    #         }
    
    #         logger.info(f"Orphan tracking for {self.file_name}: {orphan_stats}")
            
    #         # Optionally store in a separate file for reporting
    #         #self._store_orphan_metrics(orphan_stats, orphan_df)
            
    #     except Exception as e:
    #         logger.error(f"Error tracking orphans: {e}")