import os
import pandas as pd
from datetime import datetime
from utils.logger import get_logger
from utils.alerter import send_alert

logger = get_logger(__name__)

# Import Postgres quarantine writer — graceful fallback if DWH not yet initialised
try:
    from datawarehouse.quarantine_loader import write_quarantine
    _DWH_AVAILABLE = True
except Exception:
    _DWH_AVAILABLE = False


class fault_handler:
    def __init__(self, source_file: str = "unknown", source_table: str = None):
        self.base_dir     = "data/quarantine"
        self.source_file  = source_file     # passed in by validation_runner
        self.source_table = source_table    # e.g. "dim_customer", "fact_order"

    def move_to_quarantine(self, rejected_df: pd.DataFrame, reason: str, file_type: str):
        if rejected_df is None or rejected_df.empty:
            return

        count = len(rejected_df)

        # ── 1. Determine rejection stage from reason string ───────────────── #
        if "schema" in reason.lower():
            stage = "schema"
        elif "orphan" in reason.lower() or "referential" in reason.lower():
            stage = "orphan"
        else:
            stage = "records"

        # ── 2. Write to Postgres quarantine table ─────────────────────────── #
        if _DWH_AVAILABLE:
            try:
                write_quarantine(
                    df               = rejected_df,
                    source_file      = self.source_file,
                    source_table     = self.source_table or file_type,
                    rejection_reason = reason,
                    rejection_stage  = stage,
                )
            except Exception as e:
                logger.error(f"[fault_handler] Postgres quarantine write failed: {e}")

        # ── 3. Also write to CSV (existing behaviour, kept as backup) ─────── #
        try:
            quarantine_df = rejected_df.copy()
            quarantine_df['rejection_reason']    = reason
            quarantine_df['quarantine_timestamp'] = datetime.now().isoformat(sep=" ")

            safe_file_type = str(file_type).lower() if file_type else "unknown"
            target_dir     = f"{self.base_dir}/{safe_file_type}"
            os.makedirs(target_dir, exist_ok=True)

            today_date = datetime.now().strftime('%Y-%m-%d')
            file_path  = f"{target_dir}/quarantined_{today_date}.csv"

            if os.path.exists(file_path):
                quarantine_df.to_csv(file_path, mode='a', header=False, index=False)
            else:
                quarantine_df.to_csv(file_path, index=False)

            print(f"    📁 [QUARANTINE SUCCESS] Saved {count} bad records to {file_path}")
            logger.info(f"Moved {count} records to quarantine. Reason: {reason}")

            if count > 50:
                send_alert(
                    error="High Quarantine Volume Alert",
                    message=f"{count} records quarantined from {file_type} files.\nReason: {reason}"
                )

        except Exception as e:
            print(f"    ❌ [QUARANTINE ERROR] Failed to save CSV: {e}")
            logger.error(f"Failed to write quarantine CSV: {e}")
