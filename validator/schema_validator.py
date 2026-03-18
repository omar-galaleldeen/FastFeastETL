# Takes Data Frame  , file name from Ingestion Layer after parsing
#Takes Schema from dwh

class schema_validator():

    # def __init__(self , parsed_df , file_name, expected_schema):
    #     self.df = parsed_df
    #     self.file_name = file_name
    #     self.expected_schema = expected_schema


    def validate(self , df , expected_schema ):
        #checks 3 function , if all passed
            #if self.file_name in ['orders , 'tickets' , 'ticket_events']
                #batch_record_validator
            #else
                #stream_record_validator
        #else:
            #move_rejected_schema(self.file_name)
            #logger()
            #alerter()
        pass
        

    def validate_columns( self , df , expected_schema):
        #if  df_columns == schema_columns: 
            return True
        #else:
            #logger()
            #alerter()
            return False
    
    def validate_datatypes( self, df , expected_schema):
        #if df_dtypes == expected_schema.dtypes:
            return True
        #else:
            #logger()
            #alerter()
            return False

    def validate_nulls(self, df , expected_schema):
        #if mandatory columns in schema is not null 
            return True
        #else
            #logger()
            #alerter()
            return False