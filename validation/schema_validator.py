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
            #send_alert("Schema Validation" , message)
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
    
    
    def validate_datatypes(self,df , expected_schema):
        """
        Validate that each column has the expected datatype
        """
        rejected_rows = pd.DataFrame()
        
        for col in df.columns:
            expected_dtype = expected_schema.get_column(col).dtype
            if expected_dtype == 'datetime':
                converted = pd.to_datetime(df[col] , errors = 'coerce')
                mask = converted.notna()

                bad = df[~mask]
                good = df[mask].copy()
                if not bad.empty:
                    good[col] = converted[mask]

            else:
                try:
                    good = df.copy()
                    good[col] = good[col].astype(expected_dtype)
                    bad = pd.DataFrame()
                except:
                    mask = df[col].apply(lambda x: isinstance(x, eval(expected_dtype)))
                    bad = df[~mask]
                    good = df[mask]
            
            if not bad.empty:
                logger.error(f"{col} has invalid datatype rows: {len(bad)}")
                rejected_rows = pd.concat([rejected_rows, bad])
                self._handle_validation_failure(f"Found {len(bad)} invalid datatypes")

            df = good

        return df,rejected_rows
    

    def validate_nulls(self, df , expected_schema):
        """
        Validate that NOT NULL columns have no null values
        """
        rejected_rows = pd.DataFrame()
        not_null_cols = expected_schema.get_required_columns()

        for column in not_null_cols:
            mask = df[column].notna()

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
        Validate categorical columns against allowed values
        """
        rejected_rows = pd.DataFrame()
        categorical_cols = expected_schema.get_categorical_columns()

        for column in categorical_cols:
            col_name = column.name
            allowed_values = column.allowed_values
            mask = df[col_name].isin(allowed_values) | df[col_name].isna()

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
