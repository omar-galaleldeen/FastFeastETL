import os
from  validation import schema_validator as sv
from  validation import batch_records_validator as brv
from  validation import stream_records_validator as srv
from validation import orphan_validator as ov
from validation import schema_registry as sr
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)
_cfg         = get_config()
BATCH_FILES:  dict = _cfg["watcher"]["batch"]["expected_files"]
STREAM_FILES: dict = _cfg["watcher"]["stream"]["expected_files"]

def exists(file_name, files_dict):
    return any(os.path.splitext(f)[0] == file_name for f in files_dict)

#=======================================================================================#
class validation_runner:
    def __init__(self , parsed_df, file_name):
        self.df = parsed_df
        self.file_name = file_name
        self.file_type = None

        self.records_ingested = len(self.df)

        if exists(self.file_name, BATCH_FILES):
            self.file_type = "batch"
            
        elif exists(self.file_name, STREAM_FILES):
            self.file_type = "stream"

        schema_registry = sr.schema_registry()
        self.expected_schema = schema_registry.get_schema(self.file_name)



    def run(self):

        logger.info(
            f"Validation started for {self.file_name}",
            extra={
                "record_count": self.records_ingested,
                "file_type": self.file_type
            }
        )
        #fault_handler = fh.fault_handler()

        print(f"Records Ingested: {self.records_ingested} in {self.file_name}")
        schema_validation = sv.schema_validator(self.df, self.expected_schema , self.file_name)
        schema_valid , clean_df , rejected_df = schema_validation.run()

        if not schema_valid:
            print(f"Schema validation failed in {self.file_name}")
            return False
            #return False , None , rejected_df


        if not rejected_df.empty:
            print(f"Records failed in schema validation: {rejected_df.shape[0]}")
        #     fault_handler.move_to_quarantine(rejected_df, "records failed in schema validation", f"{self.file_type}")

        if self.file_type == "batch":
            batch_records_validation = brv.batch_records_validator(clean_df, self.expected_schema, self.file_name)
            batch_valid_df , batch_quarantined_df = batch_records_validation.run()
            # if not batch_quarantined_df.empty:
            #     fault_handler.move_to_quarantine(batch_quarantined_df, "quarantined_records", "batch")

            #return True , batch_valid_df , batch_quarantined_df

        elif self.file_type == "stream":
            stream_records_validation = srv.stream_records_validator(clean_df, self.expected_schema, self.file_name)
            valid_df , stream_quarantined_df= stream_records_validation.run()

            # if not stream_quarantined_df.empty:
            #     fault_handler.move_to_quarantine(stream_quarantined_df, "quarantined_records", "stream")

            orphan_validator = ov.orphan_validator(valid_df , self.file_name)
            stream_valid_df, orphan_df = orphan_validator.run()
            print(f'Valid Rows: {stream_valid_df.shape[0]}, Quarantined Rows: {orphan_df.shape[0] + stream_quarantined_df.shape[0]}') 

            # if not is_valid and orphan_df is not None:
            #     fault_handler.move_to_quarantine(orphan_df, "orphan_records", "referential")

            #return is_valid , stream_valid_df, orphan_df

        logger.info(f"Validation Ended for {self.file_name}")


        return 

    