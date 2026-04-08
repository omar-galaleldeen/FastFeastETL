import pandas as pd
import os
from pandas.api.types import is_string_dtype, is_integer_dtype, is_datetime64_any_dtype
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

    Note: update_reference_table (SQLite) has been removed.
    Dimension data is now loaded directly into Postgres by dim_loader,
    and FK lookups are served by fk_checker querying Postgres.
    """

    def __init__(self, validated_df, expected_schema, file_name):
        self.df              = validated_df
        self.file_name       = file_name
        self.expected_schema = expected_schema

    def run(self):
        """
        Execute the complete batch validation pipeline.
        Returns (valid_df, all_quarantined, duplicate_count, duplicate_rate)
        """
        pk_col          = self.expected_schema.primary_key
        all_quarantined = pd.DataFrame()

        # Null Validation
        is_nulls_valid, no_null_df, rejected_nulls = self.validate_nulls(self.df)
        if not is_nulls_valid:
            all_quarantined = pd.concat([all_quarantined, rejected_nulls])

        # Duplicate Validation
        is_dup_valid, no_dup_df, rejected_dup, duplicate_count = self.validate_duplicates(no_null_df, pk_col)
        if not is_dup_valid:
            all_quarantined = pd.concat([all_quarantined, rejected_dup])

        # Format Validation
        is_formats_valid, format_clean_df, rejected_formats = self.validate_formats(no_dup_df)
        if not is_formats_valid:
            all_quarantined = pd.concat([all_quarantined, rejected_formats])

        # Range Validation
        is_range_valid, valid_df, rejected_ranges = self.validate_value_ranges(format_clean_df)
        if not is_range_valid:
            all_quarantined = pd.concat([all_quarantined, rejected_ranges])

        if not all_quarantined.empty:
            all_quarantined = all_quarantined.drop_duplicates()

        duplicate_rate = round(duplicate_count / self.df.shape[0], 4) if self.df.shape[0] > 0 else 0.0

        print(f"Valid Rows: {valid_df.shape[0]}, Quarantined Rows: {all_quarantined.shape[0]}, Duplicates: {duplicate_count}")
        return valid_df, all_quarantined, duplicate_count, duplicate_rate

    def validate_nulls(self, df):
        null_mask  = df.isnull().any(axis=1)
        nulls_df   = df[null_mask]
        count_nulls = nulls_df.shape[0]

        if nulls_df.empty:
            return True, df, None

        columns_with_nulls = df.columns[df.isnull().any()].tolist()
        print(f"Found Nulls in {self.file_name} → {count_nulls}")
        logger.error(f"Found {count_nulls} Nulls in {self.file_name}",
                     extra={"columns_with_nulls": columns_with_nulls})
        cleaned_df = df.dropna()
        return False, cleaned_df, nulls_df

    def validate_duplicates(self, df, pk):
        duplicated_mask = df.duplicated(subset=[pk])
        duplicated_rows = df[duplicated_mask]
        count_duplicates = duplicated_rows.shape[0]

        if duplicated_rows.empty:
            return True, df, None, 0

        print(f"Found Duplicates in {self.file_name} → {count_duplicates}")
        logger.error(f"Found {count_duplicates} duplicates in {self.file_name}")
        cleaned_df = df.drop_duplicates(subset=[pk])
        return False, cleaned_df, duplicated_rows, count_duplicates

    def validate_formats(self, df):
        all_rejected_rows = pd.DataFrame()

        date_columns = ['created_at', 'updated_at', 'signup_date', 'hire_date']
        for d_col in date_columns:
            if d_col in df.columns:
                if not is_datetime64_any_dtype(df[d_col]):
                    df[d_col] = pd.to_datetime(df[d_col], format="mixed", errors='coerce')
                rejected_mask  = df[d_col].isna()
                rejected_dates = df[rejected_mask]
                if not rejected_dates.empty:
                    all_rejected_rows = pd.concat([all_rejected_rows, rejected_dates])
                    logger.error(f"Found {rejected_dates.shape[0]} invalid date formats in {self.file_name}")

        email_cols          = [col for col in df.columns if 'email' in col.lower()]
        email_regex_pattern = r'^[\w\.-]+@[\w\.-]+\.com$'
        for e_col in email_cols:
            if is_string_dtype(df[e_col]):
                rejected_mask  = ~df[e_col].str.contains(email_regex_pattern, na=False, regex=True)
                rejected_emails = df[rejected_mask]
                if not rejected_emails.empty:
                    all_rejected_rows = pd.concat([all_rejected_rows, rejected_emails])
                    logger.error(f"Found {rejected_emails.shape[0]} invalid email formats in {self.file_name}")

        phone_cols          = [col for col in df.columns if 'phone' in col.lower()]
        phone_regex_pattern = r'^0?1[0125]\d{8}$' # to suit 10 and 11 digit formats, with optional leading zero.
        for p_col in phone_cols:
            rejected_phones = pd.DataFrame()
            # Omar: ensure validation works regardless of dtype (int / float / string)
            rejected_mask  = ~df[p_col].astype(str).str.fullmatch(phone_regex_pattern)
            rejected_phones = df[rejected_mask]
            if not rejected_phones.empty:
                all_rejected_rows = pd.concat([all_rejected_rows, rejected_phones])
                logger.error(f"Found {rejected_phones.shape[0]} invalid phone formats in {self.file_name}")

        if 'national_id' in df.columns:
            rejected_mask = df['national_id'].astype(str).str.len() != 15
            rejected_ssn  = df[rejected_mask]
            if not rejected_ssn.empty:
                all_rejected_rows = pd.concat([all_rejected_rows, rejected_ssn])
                logger.error(f"Found {rejected_ssn.shape[0]} invalid national id formats in {self.file_name}")

        if not all_rejected_rows.empty:
            print(f"Found invalid formats in {self.file_name} → {all_rejected_rows.shape[0]}")
            all_rejected_rows = all_rejected_rows.drop_duplicates()
            cleaned_df = df[~df.index.isin(all_rejected_rows.index)].reset_index(drop=True)
            return False, cleaned_df, all_rejected_rows

        return True, df, None

    def validate_value_ranges(self, df):
        all_rejected_rows = pd.DataFrame()

        numeric_columns = ['segment_id', 'team_id', 'city_id', 'reason_category_id',
                           'prep_time_avg_min', 'rating_avg', 'cancel_rate',
                           'on_time_rate', 'avg_handle_time_min', 'resolution_rate', 'csat_score']
        for num_col in numeric_columns:
            if num_col in df.columns:
                numeric_series = pd.to_numeric(df[num_col], errors='coerce')
                invalid_numeric = numeric_series.isna()
                negative_values = (numeric_series < 0)
                rejected_mask   = invalid_numeric | negative_values
                if rejected_mask.any():
                    rejected_numeric = df[rejected_mask]
                    all_rejected_rows = pd.concat([all_rejected_rows, rejected_numeric])
                    logger.error(f"Found {rejected_numeric.shape[0]} invalid ranges in {self.file_name}")

        if 'rating_avg' in df.columns:
            rating_series = pd.to_numeric(df['rating_avg'], errors='coerce')
            rejected_mask = rating_series > 5
            rate_invalid  = df[rejected_mask]
            if not rate_invalid.empty:
                all_rejected_rows = pd.concat([all_rejected_rows, rate_invalid])

        rate_columns = ['cancel_rate', 'on_time_rate']
        for rate_col in rate_columns:
            if rate_col in df.columns:
                rate_series   = pd.to_numeric(df[rate_col], errors='coerce')
                rejected_mask = rate_series > 1
                ratee_invalid = df[rejected_mask]
                if not ratee_invalid.empty:
                    all_rejected_rows = pd.concat([all_rejected_rows, ratee_invalid])

        if not all_rejected_rows.empty:
            all_rejected_rows = all_rejected_rows.drop_duplicates()
            valid_df = df[~df.index.isin(all_rejected_rows.index)].reset_index(drop=True)
            return False, valid_df, all_rejected_rows

        return True, df, None