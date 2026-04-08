# Takes Data Frame from Ingestion Layer after parsing , schema , file_name
import pandas as pd
from utils.logger import get_logger
from utils.alerter import send_alert

logger = get_logger(__name__)

class schema_validator():

    def __init__(self , parsed_df , expected_schema ,  file_name):
        self.df = parsed_df
        self.file_name = file_name
        self.expected_schema = expected_schema


    def run(self):
        """
        Run schema validation - validates columns, datatypes, and null constraints.
        Returns True if valid, False if invalid (but logs errors instead of crashing)
        """
        #Structure Validation , File Level
        if not self.validate_columns(self.df, self.expected_schema):
            logger.error(f"Schema validation failed in {self.file_name} -> Didn't Pass Columns Validation")
            self._handle_validation_failure("Required Columns don't exist")
            return False , None , self.df
        
        all_rejections = pd.DataFrame()

        valid_dt_df, rejected_df_1 = self.validate_datatypes(self.df, self.expected_schema)
        valid_n_df, rejected_df_2 = self.validate_nulls(valid_dt_df, self.expected_schema)
        all_valid, rejected_df_3 = self.validate_categorical(valid_n_df, self.expected_schema)

        all_rejections = pd.concat([rejected_df_1, rejected_df_2, rejected_df_3]).drop_duplicates()


        return True, all_valid, all_rejections
    
            
    def _handle_validation_failure(self , message:str) -> None:
        """
        Handle schema validation failure - log and send alerts without crashing
        """
        try:
            send_alert(
                    error="Schema Validation failure",
                    message= f"Required Columns don't exist in {self.file_name}"
                )
            pass
        except Exception as e:
            logger.error(f"Error handling validation failure: {e}")


    def validate_columns( self , df , expected_schema):
        """
        Validate that all expected columns exist and no extra columns
        """
        if expected_schema is None:
            logger.error(f"Didn't find schema for {self.file_name}")
            return False


        df_columns = set(df.columns)
        schema_columns= set([col.name for col in expected_schema.columns])
        if df_columns != schema_columns: 
            missing_cols = schema_columns - df_columns
            extra_cols = df_columns - schema_columns
            
            if missing_cols:
                logger.error(f"Missing columns in {self.file_name}: {missing_cols}")
            if extra_cols:
                logger.error(f"Extra columns in {self.file_name}: {extra_cols}")
                    
            return False
        
        return True
    
    
    def validate_datatypes(self, df, expected_schema):
        """
        Validate that each column has the expected datatype.

        ROOT CAUSE FIX:
        file_reader reads everything as str (dtype=str for CSV, .astype(str) for JSON).
        This means JSON null → Python None → string "None", and NaN → "nan".
        The old code did astype(expected_dtype) which failed on "None", then fell into
        isinstance(x, eval(dtype)) — but isinstance("12345", int) is ALWAYS False
        because every value is a string. One null in a column = ALL rows rejected.

        Fix: use pd.to_numeric / pd.to_datetime with errors='coerce', and only reject
        rows whose value was not null-equivalent but still couldn't be converted.
        Null-equivalent strings ("None", "nan", "") are passed through so that
        validate_nulls can enforce NOT NULL constraints per the schema.
        """
        # Strings that represent null after file_reader's astype(str)
        NULL_STRINGS = {"None", "nan", "NaN", "NaT", ""}

        BOOL_TRUE  = {"True", "true", "1"}
        BOOL_FALSE = {"False", "false", "0"}

        rejected_rows = pd.DataFrame()

        for col in df.columns:
            expected_dtype = expected_schema.get_column(col).dtype

            # Rows whose raw value is null-equivalent — let validate_nulls handle them
            null_mask = df[col].isna() | df[col].isin(NULL_STRINGS)
            non_null  = ~null_mask

            if expected_dtype == "str":
                # Already strings — nothing to convert, never reject
                bad  = pd.DataFrame()
                good = df.copy()

            elif expected_dtype == "datetime":
                # Replace null strings with NaT so pd.to_datetime treats them as missing
                series    = df[col].where(non_null, other=pd.NaT)
                converted = pd.to_datetime(series, errors="coerce")
                # Bad = had a real value but conversion produced NaT (unreadable format)
                invalid_mask = non_null & converted.isna()
                bad  = df[invalid_mask]
                good = df[~invalid_mask].copy()
                good[col] = converted[~invalid_mask]   # store actual datetime dtype

            elif expected_dtype in ("int", "float"):
                # Replace null strings with NaN so pd.to_numeric treats them as missing
                series    = df[col].where(non_null, other=pd.NA)
                converted = pd.to_numeric(series, errors="coerce")
                # Bad = had a real value but couldn't parse as number (e.g. "abc")
                invalid_mask = non_null & converted.isna()
                bad  = df[invalid_mask]
                good = df[~invalid_mask].copy()
                good[col] = converted[~invalid_mask]

            elif expected_dtype == "bool":
                valid_bool = BOOL_TRUE | BOOL_FALSE
                # Bad = had a real value but not a recognised bool string
                invalid_mask = non_null & ~df[col].isin(valid_bool)
                bad  = df[invalid_mask]
                good = df[~invalid_mask].copy()
                # Convert valid bool strings to actual Python bools
                good[col] = good[col].map(
                    lambda x: True  if x in BOOL_TRUE  else
                              False if x in BOOL_FALSE else x
                )

            else:
                bad  = pd.DataFrame()
                good = df.copy()

            if not bad.empty:
                logger.error(f"{col} has invalid datatype rows: {len(bad)}")
                rejected_rows = pd.concat([rejected_rows, bad])
                self._handle_validation_failure(f"Found {len(bad)} invalid datatypes")

            df = good

        return df, rejected_rows
    

    def validate_nulls(self, df , expected_schema):
        """
        Validate that NOT NULL columns have no null values.
        Also treats null-equivalent strings ("None", "nan", "") as null,
        because file_reader converts JSON null → "None" via astype(str).
        """
        NULL_STRINGS = {"None", "nan", "NaN", "NaT", ""}
        rejected_rows = pd.DataFrame()
        not_null_cols = expected_schema.get_required_columns()

        for column in not_null_cols:
            # A value is null if it's NaN OR one of the null-equivalent strings
            mask = df[column].notna() & ~df[column].isin(NULL_STRINGS)

            bad = df[~mask]
            good = df[mask]

            if not bad.empty:
                logger.error(f"{column} has nulls: {len(bad)}")
                self._handle_validation_failure(f"Found {len(bad)} Nulls in Required Columns")
                rejected_rows = pd.concat([rejected_rows, bad])
            
            df = good 
            
        rejected_rows = rejected_rows.drop_duplicates()
                
        return df , rejected_rows
    
    def validate_categorical(self, df , expected_schema):
        """
        Validate categorical columns against allowed values.
        
        Treats null-equivalent strings ("None", "nan", "") as null values —
        these are valid for nullable categorical columns (e.g. old_status on
        the first ticket_event always has old_status=None from the generator).
        Without this fix, exactly 1/3 of ticket_events fail schema validation
        because every ticket creation event has old_status=None → "None" string
        → rejected as invalid categorical value.
        """
        NULL_STRINGS = {"None", "nan", "NaN", "NaT", ""}
        rejected_rows = pd.DataFrame()
        categorical_cols = expected_schema.get_categorical_columns()

        for column in categorical_cols:
            col_name = column.name
            allowed_values = column.allowed_values
            # Treat null-equivalent strings the same as NaN — valid for nullable cols
            is_null_equivalent = df[col_name].isin(NULL_STRINGS)
            mask = df[col_name].isin(allowed_values) | df[col_name].isna() | is_null_equivalent

            bad = df[~mask]
            good = df[mask]

            if not bad.empty:
                logger.error(f"{col_name} has invalid categorical values: {len(bad)}")
                self._handle_validation_failure(f"Found {len(bad)} invalid Categorical values")
                rejected_rows = pd.concat([rejected_rows, bad])
            
            df = good 
        
        #if a row has more than 1 categorical column
        rejected_rows = rejected_rows.drop_duplicates()
                
        return df , rejected_rows