import os
import pandas as pd
from datetime import datetime
from utils.logger import get_logger
from utils.alerter import send_alert

logger = get_logger(__name__)

class fault_handler:
    def __init__(self):
        # Setup quarantine directories for bad data
        self.base_dir = "data/quarantine"
        os.makedirs(f"{self.base_dir}/batch", exist_ok=True)
        os.makedirs(f"{self.base_dir}/stream", exist_ok=True)

    def move_to_quarantine(self, rejected_df: pd.DataFrame, reason: str, file_type: str):
        """
        Appends rejected records to a daily quarantine CSV file along with the 
        rejection reason and timestamp.
        """
        # Do nothing if the dataframe is empty or None
        if rejected_df is None or rejected_df.empty:
            return

        count = len(rejected_df)
        
        try:
            # Create a copy to avoid SettingWithCopyWarning
            quarantine_df = rejected_df.copy()
            
            # Add audit columns
            quarantine_df['rejection_reason'] = reason
            quarantine_df['quarantine_timestamp'] = datetime.now().isoformat(sep=" ")
            
            # Determine file path based on current date and file type (batch/stream)
            today_date = datetime.now().strftime('%Y-%m-%d')
            file_path = f"{self.base_dir}/{file_type}/quarantined_{today_date}.csv"
            
            # Append if file exists, otherwise write with header
            if os.path.exists(file_path):
                quarantine_df.to_csv(file_path, mode='a', header=False, index=False)
            else:
                quarantine_df.to_csv(file_path, index=False)
                
            # Log the action
            logger.info(f"Moved {count} records to quarantine. Reason: {reason}")
            
            # Alerting logic: Trigger an email if a large batch of records fails
            if count > 50:
                send_alert(
                    error="High Quarantine Volume Alert", 
                    message=f"{count} records were just quarantined from {file_type} files.\nReason: {reason}"
                )
                
        except Exception as e:
            # Log the error but never crash the pipeline
            logger.error(f"Failed to write to quarantine file: {e}")