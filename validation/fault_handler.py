import os
import pandas as pd
from datetime import datetime
from utils.logger import get_logger
from utils.alerter import send_alert

logger = get_logger(__name__)

class fault_handler:
    def __init__(self):
        self.base_dir = "data/quarantine"

    def move_to_quarantine(self, rejected_df: pd.DataFrame, reason: str, file_type: str):
        if rejected_df is None or rejected_df.empty:
            return

        count = len(rejected_df)
        
        try:
            quarantine_df = rejected_df.copy()
            quarantine_df['rejection_reason'] = reason
            quarantine_df['quarantine_timestamp'] = datetime.now().isoformat(sep=" ")
            
            # 1. Ensure file_type is a valid string, fallback to "unknown" if None
            safe_file_type = str(file_type).lower() if file_type else "unknown"
            
            # 2. DYNAMICALLY create the target folder so it never fails
            target_dir = f"{self.base_dir}/{safe_file_type}"
            os.makedirs(target_dir, exist_ok=True)
            
            # 3. Save the file
            today_date = datetime.now().strftime('%Y-%m-%d')
            file_path = f"{target_dir}/quarantined_{today_date}.csv"
            
            if os.path.exists(file_path):
                quarantine_df.to_csv(file_path, mode='a', header=False, index=False)
            else:
                quarantine_df.to_csv(file_path, index=False)
                
            # 4. LOUD PRINT: So you can see it in the terminal!
            print(f"    📁 [QUARANTINE SUCCESS] Saved {count} bad records to {file_path}")
            logger.info(f"Moved {count} records to quarantine. Reason: {reason}")
            
            if count > 50:
                send_alert(
                    error="High Quarantine Volume Alert", 
                    message=f"{count} records were just quarantined from {file_type} files.\nReason: {reason}"
                )
                
        except Exception as e:
            # Print the exact error to the terminal if it fails
            print(f"    ❌ [QUARANTINE ERROR] Failed to save file: {e}")
            logger.error(f"Failed to write to quarantine file: {e}")