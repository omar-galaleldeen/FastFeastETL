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
class validator_orchestrator:
    def __init__(self , parsed_df, file_name):
        self.df = parsed_df
        self.file_name = file_name
        self.file_type = None


        if exists(self.file_name, BATCH_FILES):
            self.file_type = "batch"
            #file_config = BATCH_FILES[self.file_name]
            
        elif exists(self.file_name, STREAM_FILES):
            self.file_type = "stream"
            #file_config = STREAM_FILES[self.file_name]

        #self.file = self.file_name.split('.')[0]
        schema_registry = sr.schema_registry()
        self.expected_schema = schema_registry.get_schema(self.file_name)



    def run(self):

        logger.info(f"Validation Started for {self.file_name}")
        print(f"\n===== Running Schema Validation for {self.file_name} =====")
        schema_validation = sv.schema_validator(self.df, self.expected_schema , self.file_name)
        schema_valid , clean_df , rejected_df = schema_validation.run()

        if not schema_valid:
            print(f"Schema validation failed in {self.file_name}")
            return False

        if self.file_type == "batch":
            batch_records_validation = brv.batch_records_validator(clean_df, self.expected_schema, self.file_name)
            batch_valid_df , batch_quarantined_df = batch_records_validation.run()
            # if not batch_quarantined_df.empty:
            #     fault_handler = fh.fault_handler()
            #     fault_handler.move_to_quarantine(batch_quarantined_df, "quarantined_records", "batch")
            #return batch_valid_df
            print("================= Validation Completed =================\n")

        elif self.file_type == "stream":
            stream_records_validation = srv.stream_records_validator(clean_df, self.expected_schema, self.file_name)
            valid_df , stream_quarantined_df= stream_records_validation.run()

            # if not stream_quarantined_df.empty:
            #     fault_handler = fh.fault_handler()
            #     fault_handler.move_to_quarantine(stream_quarantined_df, "quarantined_records", "stream")

            orphan_validator = ov.orphan_validator(valid_df , self.file_name)
            orphan_validator.run()
            #is_valid, stream_valid_df, orphan_df = orphan_validator.run()

            # if not is_valid and orphan_df is not None:
            #     fault_handler = fh.fault_handler()
            #     fault_handler.move_to_quarantine(orphan_df, "orphan_records", "referential")

            #return stream_valid_df
            print("================= Validation Completed =================\n")

        logger.info(f"Validation Ended for {self.file_name}")


        return 

    