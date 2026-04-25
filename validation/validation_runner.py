import os
from datetime import datetime
from validation import schema_validator as sv
from validation import batch_records_validator as brv
from validation import stream_records_validator as srv
from validation import orphan_validator as ov
from validation import schema_registry as sr
from validation.fault_handler import fault_handler
from validation.pii_handler import pii_handler
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)
_cfg         = get_config()
BATCH_FILES:  dict = _cfg["watcher"]["batch"]["expected_files"]
STREAM_FILES: dict = _cfg["watcher"]["stream"]["expected_files"]


class validation_runner:
    def __init__(self, parsed_df, file_name, file_path: str = None, source_table: str = None):
        self.df           = parsed_df
        self.original_file_name = file_name
        self.file_name    = os.path.splitext(file_name)[0].lower()
        self.file_path    = file_path
        self.source_table = source_table    # passed to fault_handler for quarantine metadata
        self.file_type    = "unknown"
        self.records_ingested = len(self.df)

        if self.original_file_name in BATCH_FILES or self.file_name in BATCH_FILES:
            self.file_type = "batch"
        elif self.original_file_name in STREAM_FILES or self.file_name in STREAM_FILES:
            self.file_type = "stream"

        schema_registry_obj   = sr.schema_registry()
        self.expected_schema  = schema_registry_obj.get_schema(self.file_name)

    def run(self):
        logger.info(
            f"Validation started for {self.original_file_name}",
            extra={"record_count": self.records_ingested, "file_type": self.file_type}
        )

        # fault_handler now receives source_file and source_table for Postgres quarantine
        fh = fault_handler(
            source_file  = self.original_file_name,
            source_table = self.source_table,
        )

        print(f"\n{'─' * 50}")
        print(f"📄 Processing: {self.original_file_name} ({self.file_type})")
        print(f"📂 Path: {self.file_path or 'unknown'}")
        print(f"{'─' * 50}")
        print(f"Records Ingested: {self.records_ingested} in {self.original_file_name}")

        # 1. Schema Validation
        schema_validation = sv.schema_validator(self.df, self.expected_schema, self.file_name)
        schema_valid, clean_df, rejected_df = schema_validation.run()

        if not schema_valid:
            print(f"Schema validation failed completely in {self.original_file_name}")
            fh.move_to_quarantine(rejected_df, "Records failed in schema validation", self.file_type)
            processed_timestamp = datetime.now().isoformat(sep=" ")
            return False, clean_df, self.original_file_name, processed_timestamp, 0

        if not rejected_df.empty:
            print(f"Records failed in schema validation: {rejected_df.shape[0]}")
            fh.move_to_quarantine(rejected_df, "Records failed in schema validation", self.file_type)

        valid_records_df = clean_df

        # 2. Batch or Stream specific validation
        if self.file_type == "batch":
            batch_records_validation = brv.batch_records_validator(
                clean_df, self.expected_schema, self.file_name
            )
            batch_valid_df, batch_quarantined_df, duplicate_count, duplicate_rate = batch_records_validation.run()

            if not batch_quarantined_df.empty:
                fh.move_to_quarantine(batch_quarantined_df, "Quarantined records due to batch rules", "batch")

            logger.info(
                f"Duplicate metrics for {self.original_file_name}",
                extra={
                    "duplicate_count":    duplicate_count,
                    "duplicate_rate_pct": duplicate_rate,
                    "total_records":      self.records_ingested,
                }
            )
            valid_records_df = batch_valid_df
            orphan_count = 0  # batch files are not subject to orphan checks

        elif self.file_type == "stream":
            stream_records_validation = srv.stream_records_validator(
                clean_df, self.expected_schema, self.file_name
            )
            stream_valid_df, stream_quarantined_df, duplicate_count, duplicate_rate = stream_records_validation.run()

            if not stream_quarantined_df.empty:
                fh.move_to_quarantine(stream_quarantined_df, "Quarantined records due to stream rules", "stream")

            logger.info(
                f"Duplicate metrics for {self.original_file_name}",
                extra={
                    "duplicate_count":    duplicate_count,
                    "duplicate_rate_pct": duplicate_rate,
                    "total_records":      self.records_ingested,
                }
            )

            orphan_validator_obj = ov.orphan_validator(stream_valid_df, self.file_name)
            final_stream_valid_df, orphan_df = orphan_validator_obj.run()

            orphan_count = len(orphan_df) if orphan_df is not None and not orphan_df.empty else 0

            if orphan_df is not None and not orphan_df.empty:
                fh.move_to_quarantine(
                    orphan_df,
                    "Orphan records (Referential integrity failed)",
                    "stream"
                )

            valid_records_df = final_stream_valid_df

        logger.info(f"Validation Ended for {self.original_file_name}")

        # 3. PII Handling
        pii_processor = pii_handler()
        if valid_records_df is not None:
            secured_records_df = pii_processor.mask_pii(valid_records_df, self.file_name)
        else:
            secured_records_df = valid_records_df

        processed_timestamp = datetime.now().isoformat(sep=" ")
        return True, secured_records_df, self.original_file_name, processed_timestamp, orphan_count
