import hashlib
import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

class pii_handler:
    def __init__(self):
        # Dictionary defining which columns contain PII for each file
        self.pii_columns = {
            "customers": ["full_name", "email", "phone"],
            "drivers": ["driver_name", "driver_phone", "national_id"],
            "agents": ["agent_name", "agent_email", "agent_phone"]
        }

    def _hash_value(self, val):
        """
        Hashes a single value using SHA-256. 
        Returns None if the value is empty or NaN.
        """
        if pd.isna(val) or val == "" or str(val).strip().lower() == "n/a":
            return val
        
        # Encode to bytes, hash, and return the hex string
        return hashlib.sha256(str(val).encode('utf-8')).hexdigest()

    def mask_pii(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """
        Applies SHA-256 hashing to PII columns based on the file name.
        """
        if df is None or df.empty:
            return df

        # Check if this file has PII columns defined
        cols_to_mask = self.pii_columns.get(file_name, [])
        if not cols_to_mask:
            return df  # Return as-is if no PII

        logger.info(f"Masking PII columns for {file_name}: {cols_to_mask}")
        
        masked_df = df.copy()
        
        for col in cols_to_mask:
            if col in masked_df.columns:
                # Apply the hash function to the entire column
                masked_df[col] = masked_df[col].apply(self._hash_value)
                
        return masked_df