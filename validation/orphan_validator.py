"""
orphan_validator.py
===================
Checks FK constraints for stream records.
FK lookups now go to Postgres (via fk_checker) instead of SQLite reference.db.
SQLite reference.db is retired — no longer read or written here.
"""
import pandas as pd
import os
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)


class orphan_validator:

    def __init__(self, valid_df, file_name):
        self.df        = valid_df
        self.file_name = file_name

    def run(self):
        """
        Run referential validation (orphan checking) for stream files.
        Returns (valid_df, orphan_df)
        """
        is_orphan_valid, valid_df, orphan_df = self.check_orphans(self.df, self.file_name)

        if not is_orphan_valid:
            count = orphan_df.shape[0] if orphan_df is not None else 0
            print(f"Found {count} Orphans in {self.file_name} & moving to quarantine")

        return valid_df, orphan_df

    def check_orphans(self, df, file_name):
        """
        Check for orphaned records using Postgres FK lookups via fk_checker.
        """
        fk_rules = self._get_foreign_key_rules(file_name)

        if not fk_rules:
            return True, df, None

        # Import here to avoid circular import at module load time
        try:
            from datawarehouse.fk_checker import check_fk, resolve_ref_table
        except Exception as exc:
            logger.error(f"[orphan_validator] fk_checker unavailable: {exc}. Skipping FK checks.")
            return True, df, None

        orphan_df = pd.DataFrame()

        for fk_col, logical_ref in fk_rules.items():
            ref_info = resolve_ref_table(logical_ref)
            if ref_info is None:
                logger.warning(
                    f"[orphan_validator] No Postgres mapping for '{logical_ref}' — skipping."
                )
                continue

            ref_table, ref_pk = ref_info

            try:
                df, orphans = check_fk(df, fk_col, ref_table, ref_pk)
                if not orphans.empty:
                    orphan_df = pd.concat([orphan_df, orphans])
                    logger.error(
                        f"Found {orphans.shape[0]} orphans in {file_name} on '{fk_col}' "
                        f"→ {ref_table}.{ref_pk}"
                    )
            except Exception as exc:
                logger.error(f"[orphan_validator] FK check failed for '{fk_col}': {exc}")
                continue

        if not orphan_df.empty:
            orphan_df = orphan_df.drop_duplicates()
            return False, df, orphan_df

        return True, df, None

    def _get_foreign_key_rules(self, file_name):
        """Get FK rules for the file from config."""
        try:
            _cfg = get_config()
            STREAM_FILES = _cfg.get("watcher", {}).get("stream", {}).get("expected_files", {})
            file_config  = next(
                (cfg for fname, cfg in STREAM_FILES.items()
                 if os.path.splitext(fname)[0] == file_name),
                {}
            )
            return file_config.get("foreign_keys", {})
        except Exception as exc:
            logger.error(f"[orphan_validator] Error loading FK rules: {exc}")
            return {}
