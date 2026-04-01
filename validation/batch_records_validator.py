import pandas as pd
import os
import sqlite3
from pandas.api.types import is_string_dtype , is_integer_dtype , is_datetime64_any_dtype
from utils.logger import get_logger

logger = get_logger(__name__)


class batch_records_validator():
    """
    Validates batch records for data quality after schema validation.
    
    Performs sequential validation steps:
    1. NULL validation
    2. Duplicate validation
    3. Format validation
    4. Range validation
    """

    def __init__(self , validated_df , expected_schema , file_name):
        """
        Initialize the batch records validator.
        
        Parameters
        ----------
        validated_df : pandas.DataFrame
            DataFrame containing the data to validate.
        expected_schema : Schema object
            Object containing the expected schema with column metadata including PK information.
        file_name : str
            Name of the source file being validated, used for logging and reference updates.
        """
        self.df = validated_df
        self.file_name = file_name
        self.expected_schema = expected_schema


    def run(self):
        """
        Execute the complete batch validation pipeline.
        
        Performs validations in order: nulls → duplicates → formats → ranges.
        Each validation step builds on the cleaned DataFrame from the previous step.
        """

        pk_col = self.expected_schema.primary_key
        all_quarantined = pd.DataFrame()

        #Null Validation
        is_nulls_valid, no_null_df, rejected_nulls = self.validate_nulls(self.df)
        if not is_nulls_valid:
            all_quarantined = pd.concat([all_quarantined, rejected_nulls]) 


        #Duplicate Validation
        is_dup_valid, no_dup_df, rejected_dup= self.validate_duplicates(no_null_df, pk_col)
        if not is_dup_valid:
            all_quarantined = pd.concat([all_quarantined, rejected_dup]) 


        #Format Validation
        is_formats_valid, format_clean_df , rejected_formats = self.validate_formats(no_dup_df)
        if not is_formats_valid:
            all_quarantined = pd.concat([all_quarantined, rejected_formats])

        #Range Validation
        is_range_valid , valid_df , rejected_ranges = self.validate_value_ranges(format_clean_df)
        if not is_range_valid: 
            all_quarantined = pd.concat([all_quarantined, rejected_ranges])

        if not all_quarantined.empty:
            all_quarantined = all_quarantined.drop_duplicates()

        reference_tables = ['customers' , 'drivers' , 'agents' , 'restaurants']
        if self.file_name in reference_tables:
            self.update_reference_table(valid_df)

        print(f"Valid Rows: {valid_df.shape[0]}, Quarantined Rows: {all_quarantined.shape[0]}")
        return valid_df, all_quarantined
    

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


    def validate_nulls(self, df):
        """
        Remove all rows containing any null values.
        """
        null_mask = df.isnull().any(axis=1)
        nulls_df = df[null_mask]
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
        cleaned_df = df.dropna()
        return False , cleaned_df, nulls_df


    def validate_duplicates(self, df , pk):
        """
        Remove duplicate records based on primary key.
        """

        duplicated_mask = df.duplicated(subset=[pk])
        duplicated_rows = df[duplicated_mask]
        count_duplicates = duplicated_rows.shape[0]

        if duplicated_rows.empty:
            return True , df, None
        
        print(f"Found Duplicates in {self.file_name} → {count_duplicates}")
        logger.error(f"Found {count_duplicates} duplicates in  {self.file_name}")
        #move_to_quarantine(duplicated_rows , "duplicated rows" , f"{self.file_name}")
        cleaned_df = df.drop_duplicates(subset=[pk])
        return False , cleaned_df , duplicated_rows
    


    def validate_formats(self, df):
        """
        Validate data formats for dates, emails, phone numbers, and national IDs.
        """
         
        all_rejected_rows=pd.DataFrame()

        date_columns = ['created_at' , 'updated_at' , 'signup_date' , 'hire_date']
        for d_col in date_columns:
            if d_col in df.columns:
                if not is_datetime64_any_dtype(df[d_col]):
                    df[d_col] = pd.to_datetime(df[d_col], format="mixed" , errors='coerce')
                rejected_mask = df[d_col].isna()
                rejected_dates = df[rejected_mask]
                if not rejected_dates.empty:
                    all_rejected_rows = pd.concat([all_rejected_rows, rejected_dates])
                    logger.error(f"Found {rejected_dates.shape[0]} invalid date formats in  {self.file_name}:")



        email_cols = [col for col in df.columns if 'email' in col.lower()]
        email_regex_pattern = r'^[\w\.-]+@[\w\.-]+\.com$'
        for e_col in email_cols:
            if is_string_dtype(df[e_col]):
                rejected_mask = ~df[e_col].str.contains(email_regex_pattern, na=False , regex=True)
                rejected_emails = df[rejected_mask]
                if not rejected_emails.empty:
                    all_rejected_rows = pd.concat([all_rejected_rows, rejected_emails])
                    logger.error(f"Found {rejected_emails.shape[0]} invalid email formats in  {self.file_name}:")


        phone_cols = [col for col in df.columns if 'phone' in col.lower()]
        phone_regex_pattern = r'^01[0125]\d{8}$'
        for p_col in phone_cols:
            rejected_phones = pd.DataFrame()   # BUG FIX: initialize per-iteration to prevent UnboundLocalError

            if is_string_dtype(df[p_col]):
                rejected_mask= ~df[p_col].str.contains(phone_regex_pattern, na=False , regex=True)
                rejected_phones= df[rejected_mask]

            elif is_integer_dtype(df[p_col]):
                rejected_mask = df[p_col].astype(str).str.len() != 11  # BUG FIX: Egyptian numbers are 11 digits, not 10
                rejected_phones= df[rejected_mask]

            if not rejected_phones.empty:
                all_rejected_rows = pd.concat([all_rejected_rows,rejected_phones])
                logger.error(f"Found {rejected_phones.shape[0]} invalid phone formats in  {self.file_name}:")

        if 'national_id' in df.columns:
            rejected_mask = df['national_id'].astype(str).str.len() != 15
            rejected_ssn = df[rejected_mask]
            if not rejected_ssn.empty:
                all_rejected_rows = pd.concat([all_rejected_rows,rejected_ssn])
                logger.error(f"Found {rejected_ssn.shape[0]} invalid national id formats in  {self.file_name}:")


        
        if not all_rejected_rows.empty:
            print(f"Found invalid formats in {self.file_name} → {all_rejected_rows.shape[0]}")
            all_rejected_rows = all_rejected_rows.drop_duplicates()
            #move_to_quarantine(all_rejected_rows , "invalid formats", f"{self.file_name}")
            cleaned_df = df.drop(all_rejected_rows.index)
            return False, cleaned_df , all_rejected_rows
        

        return True, df , None


    def validate_value_ranges(self, df):
        """
        Validate numeric values are within acceptable business ranges.
        Validates:
        - All numeric values must be >= 0
        - rating_avg must be between 0 and 5
        - cancel_rate and on_time_rate must be between 0 and 1
        """

        all_rejected_rows=pd.DataFrame()

        numeric_columns =  [ 'segment_id', 'team_id', 'city_id', 'reason_category_id', 'prep_time_avg_min', 'rating_avg', 'cancel_rate', 'on_time_rate', 'avg_handle_time_min', 'resolution_rate', 'csat_score']
        for num_col in numeric_columns:
            if num_col in df.columns:
                numeric_series = pd.to_numeric(df[num_col], errors='coerce')
                invalid_numeric = numeric_series.isna()
                negative_values = (numeric_series < 0)
                rejected_mask = invalid_numeric | negative_values

                if rejected_mask.any():
                    rejected_numeric = df[rejected_mask]
                    all_rejected_rows = pd.concat([all_rejected_rows , rejected_numeric])
                    logger.error(f"Found {rejected_numeric.shape[0]} invalid ranges in  {self.file_name}:")
                    
                
        if 'rating_avg' in df.columns:
            rating_series = pd.to_numeric(df['rating_avg'], errors='coerce')
            rejected_mask = rating_series > 5
            rate_invalid = df[rejected_mask]
            if not rate_invalid.empty:
                all_rejected_rows = pd.concat([all_rejected_rows, rate_invalid])
                logger.error(f"Found {rate_invalid.shape[0]} invalid ranges in  {self.file_name}:")

        

        rate_columns = ['cancel_rate' , 'on_time_rate']
        for rate_col in rate_columns:
            if rate_col in df.columns:
                rate_series = pd.to_numeric(df[rate_col], errors='coerce')
                rejected_mask = rate_series > 1
                ratee_invalid = df[rejected_mask]
                if not ratee_invalid.empty:
                    all_rejected_rows = pd.concat([all_rejected_rows, ratee_invalid])
                    logger.error(f"Found {ratee_invalid.shape[0]} invalid ranges in  {self.file_name}:")

            

        if not all_rejected_rows.empty:
            all_rejected_rows = all_rejected_rows.drop_duplicates()
            #move_to_quarantine(all_rejected_rows , "invalid ranges" , f"{self.file_name}")
            valid_df = df.drop(all_rejected_rows.index)
            return False , valid_df, all_rejected_rows
        
        
        return True , df, None