from  validation import schema_validator as sv
from  validation import records_validator as rv

class validator_orchestrator:

    def __init__(self , parsed_df , expected_schema):
        self.df = parsed_df
        self.expected_schema = expected_schema

        self.schema_v = sv.schema_validator()
        self.records_v = rv.records_validator()

    def run():
        #run validate in schema_validator

        #run validate_records in records_validator

        return