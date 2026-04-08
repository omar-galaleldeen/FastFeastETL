import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

class stream_records_validator():
    """
    Validates stream records for data quality after schema validation.
    
    Performs sequential validation steps:
    1. NULL validation
    2. Duplicate validation
    3. Format validation
    4. Range validation
    """

    def __init__(self , valid_df , expected_schema , file_name):
        """
        Initialize the stream records validator.
        
        Parameters
        ----------
        validated_df : pandas.DataFrame
            DataFrame containing the data to validate.
        expected_schema : Schema object
            Object containing the expected schema with column metadata including PK information.
        file_name : str
            Name of the source file being validated, used for logging and reference updates.
        """
        self.df = valid_df
        self.file_name = file_name
        self.expected_schema = expected_schema


    def run(self):
        """
        Execute the complete stream validation pipeline.
        Performs validations in order: nulls → duplicates → formats → ranges.

        Returns the validated DataFrame for referential validation.
        """
        pk_col = self.expected_schema.primary_key
        all_quarantined = pd.DataFrame()
        duplicate_count = 0  

        # Null Validation
        is_nulls_valid, no_null_df, rejected_nulls = self.validate_nulls(self.df)
        if not is_nulls_valid and rejected_nulls is not None:
            all_quarantined = pd.concat([all_quarantined, rejected_nulls]) 

        # Empty Validation 
        is_empty_valid, no_empty_df, rejected_empty = self.validate_empty(no_null_df)
        if not is_empty_valid and rejected_empty is not None:
            all_quarantined = pd.concat([all_quarantined, rejected_empty])

        # Duplicate Validation 
        is_dup_valid, no_dup_df, rejected_dup, duplicate_count = self.validate_duplicates(no_empty_df, pk_col)
        if not is_dup_valid and rejected_dup is not None:
            all_quarantined = pd.concat([all_quarantined, rejected_dup])

        # Format Validation
        is_formats_valid, format_clean_df , rejected_formats = self.validate_formats(no_dup_df)
        if not is_formats_valid and rejected_formats is not None:
            all_quarantined = pd.concat([all_quarantined, rejected_formats])

        # Range Validation
        is_range_valid , valid_df , rejected_ranges = self.validate_value_ranges(format_clean_df)
        if not is_range_valid and rejected_ranges is not None:
            all_quarantined = pd.concat([all_quarantined, rejected_ranges])

        if not all_quarantined.empty:
            all_quarantined = all_quarantined.drop_duplicates()

        duplicate_rate = round(duplicate_count / self.df.shape[0], 4) if self.df.shape[0] > 0 else 0.0

        print(f"Valid Rows: {valid_df.shape[0]}, Quarantined Rows: {all_quarantined.shape[0]}, Duplicates: {duplicate_count}")
        return valid_df, all_quarantined, duplicate_count, duplicate_rate


    def validate_nulls(self, df):
        """
        Remove rows with null values, ignoring specified columns.
        
        For stream data, old_status can be null as it represents the previous
        state which may not exist for new records.
        """
        ignore_cols = ['old_status']

        cols_to_check = [col for col in df.columns if col not in ignore_cols]

        nulls_mask = df[cols_to_check].isnull().any(axis=1)
        nulls_df = df[nulls_mask]
        count_nulls = nulls_df.shape[0]
        
        if nulls_df.empty:
            return True , df, None
        
         # Get columns that have null values
        columns_with_nulls = df.columns[df.isnull().any()].tolist()
        
        print(f"Found Nulls in {self.file_name} → {count_nulls}")
        logger.error(f"Found {count_nulls} Nulls in  {self.file_name}",
                     extra = {
                         "columns_with_nulls": columns_with_nulls
                         })
        #move_to_quarantine(nulls_df , "null values" , f"{self.file_name}")
        cleaned_df = df.dropna(subset=cols_to_check).copy()  # .copy() prevents SettingWithCopyWarning
        return False , cleaned_df, nulls_df


    def validate_duplicates(self, df , pk):
        """
        Remove duplicate records based on primary key.
        Returns (is_valid, clean_df, rejected_df, duplicate_count)
        """
        duplicated_mask = df.duplicated(subset=[pk])
        duplicated_rows = df[duplicated_mask]
        count_duplicates = duplicated_rows.shape[0]

        if duplicated_rows.empty:
            return True , df, None, 0
        
        print(f"Found duplicates in {self.file_name} → {count_duplicates}")
        logger.error(f"Found {count_duplicates} duplicates in  {self.file_name}")
        #move_to_quarantine(duplicated_rows , "duplicated rows" , f"{self.file_name}")
        cleaned_df = df.drop_duplicates(subset=[pk]).copy()  # .copy() prevents SettingWithCopyWarning
        return False , cleaned_df , duplicated_rows, count_duplicates
    


    def validate_formats(self,df):
        """
        Validate date formats for streaming-specific columns.
        """
        all_rejected_rows=pd.DataFrame()

        date_columns = ['order_created_at', 'delivered_at', 'event_ts', 'created_at', 'first_response_at', 'resolved_at', 'sla_first_due_at', 'sla_resolve_due_at']
        for d_col in date_columns:
            if d_col in df.columns:
                df[d_col] = pd.to_datetime(df[d_col], format="mixed" , errors='coerce')
                rejected_mask = df[d_col].isna()
                rejected_dates = df[rejected_mask]
                if not rejected_dates.empty:
                    all_rejected_rows = pd.concat([all_rejected_rows,rejected_dates])
                    logger.error(f"Found {rejected_dates.shape[0]} invalid date formats in  {self.file_name}:")


        if not all_rejected_rows.empty:
            print(f"Found invalid formats in {self.file_name} → {all_rejected_rows.shape[0]}")
            all_rejected_rows = all_rejected_rows.drop_duplicates()
            #move_to_quarantine(all_rejected_rows , "invalid formats", f"{self.file_name}")
            cleaned_df = df.drop(all_rejected_rows.index).copy()  # .copy() prevents SettingWithCopyWarning
            return False, cleaned_df , all_rejected_rows
        

        return True, df , None
    
    def validate_value_ranges(self,df):
        """
        Validate numeric values are within acceptable business ranges.
        - All monetary values must be >= 0
        """
        all_rejected_rows = pd.DataFrame()

        numeric_cols = ['order_amount' , 'delivery_fee' , 'discount_amount', 'total_amount' , 'refund_amount']
        for n_col in numeric_cols:
            if n_col in df.columns:
                numeric_series = pd.to_numeric(df[n_col], errors='coerce')
                invalid_numeric = numeric_series.isna()
                negative_values = (numeric_series < 0)
                rejected_mask = invalid_numeric | negative_values

                if rejected_mask.any():
                    rejected_numeric = df[rejected_mask]
                    all_rejected_rows = pd.concat([all_rejected_rows , rejected_numeric])
                    logger.error(f"Found {rejected_numeric.shape[0]} invalid ranges in  {self.file_name}:")

        
        if not all_rejected_rows.empty:
            all_rejected_rows = all_rejected_rows.drop_duplicates()
            #move_to_quarantine(rejected_df , "invalid ranges" , f"{self.file_name}")
            valid_df = df.drop(all_rejected_rows.index)
            return False, valid_df ,all_rejected_rows
        
        return True , df , None
    
    def validate_empty(self, df):
        str_cols = df.select_dtypes(include=['object', 'string']).columns
        if len(str_cols) == 0:
            return True, df, None

        empty_mask = df[str_cols].apply(lambda col: col.astype(str).str.strip() == "").any(axis=1)

        empty_rows = df[empty_mask]
        count_empty = empty_rows.shape[0]

        if empty_rows.empty:
            return True, df, None

        print(f"Found Empty Values in {self.file_name} → {count_empty}")
        logger.error(f"Found {count_empty} empty values in {self.file_name}")

        cleaned_df = df[~empty_mask].copy()

        return False, cleaned_df, empty_rows