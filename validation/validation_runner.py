import os
from validation import schema_validator as sv
from validation import batch_records_validator as brv
from validation import stream_records_validator as srv
from validation import orphan_validator as ov
from validation import schema_registry as sr
from validation.fault_hander import fault_handler
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)
_cfg         = get_config()
BATCH_FILES:  dict = _cfg["watcher"]["batch"]["expected_files"]
STREAM_FILES: dict = _cfg["watcher"]["stream"]["expected_files"]

def exists(file_name, files_dict):
    return any(os.path.splitext(f)[0] == file_name for f in files_dict)


class validation_runner:
    def __init__(self, parsed_df, file_name):
        self.df = parsed_df
        self.file_name = file_name
        self.file_type = None

        self.records_ingested = len(self.df)

        if exists(self.file_name, BATCH_FILES):
            self.file_type = "batch"
            
        elif exists(self.file_name, STREAM_FILES):
            self.file_type = "stream"

        schema_registry_obj = sr.schema_registry()
        self.expected_schema = schema_registry_obj.get_schema(self.file_name)


    def run(self):

        logger.info(
            f"Validation started for {self.file_name}",
            extra={
                "record_count": self.records_ingested,
                "file_type": self.file_type
            }
        )
        
        fh = fault_handler()

        print(f"Records Ingested: {self.records_ingested} in {self.file_name}")
        
        # 1. Schema Validation
        schema_validation = sv.schema_validator(self.df, self.expected_schema , self.file_name)
        schema_valid, clean_df, rejected_df = schema_validation.run()

        # If schema is completely invalid, quarantine the whole thing
        if not schema_valid:
            print(f"Schema validation failed completely in {self.file_name}")
            fh.move_to_quarantine(rejected_df, "Records failed in schema validation", f"{self.file_type}")
            return False, clean_df

        # If some records failed schema but others are fine
        if not rejected_df.empty:
            print(f"Records failed in schema validation: {rejected_df.shape[0]}")
            fh.move_to_quarantine(rejected_df, "Records failed in schema validation", f"{self.file_type}")

        valid_records_df = clean_df

        # 2. Batch or Stream specific validation
        if self.file_type == "batch":
            batch_records_validation = brv.batch_records_validator(clean_df, self.expected_schema, self.file_name)
            batch_valid_df, batch_quarantined_df = batch_records_validation.run()
            
            if not batch_quarantined_df.empty:
                fh.move_to_quarantine(batch_quarantined_df, "Quarantined records due to batch rules", "batch")

            valid_records_df = batch_valid_df

        elif self.file_type == "stream":
            stream_records_validation = srv.stream_records_validator(clean_df, self.expected_schema, self.file_name)
            stream_valid_df, stream_quarantined_df = stream_records_validation.run()

            if not stream_quarantined_df.empty:
                fh.move_to_quarantine(stream_quarantined_df, "Quarantined records due to stream rules", "stream")

            orphan_validator_obj = ov.orphan_validator(stream_valid_df, self.file_name)
            final_stream_valid_df, orphan_df = orphan_validator_obj.run()
            
            print(f'Valid Rows: {final_stream_valid_df.shape[0] if final_stream_valid_df is not None else 0}, Quarantined Rows: {(orphan_df.shape[0] if orphan_df is not None else 0) + (stream_quarantined_df.shape[0] if stream_quarantined_df is not None else 0)}') 

            if orphan_df is not None and not orphan_df.empty:
                fh.move_to_quarantine(orphan_df, "Orphan records (Referential integrity failed)", "stream")

            valid_records_df = final_stream_valid_df

        logger.info(f"Validation Ended for {self.file_name}")

        # Return success status and the clean dataframe for the next layer (DWH loading)
        return True, valid_records_df