class records_validator():

    # def __init__(self , validated_df , expected_schema , file_name):
    #     self.df = validated_df
    #     self.file_name = file_name
    #     self.expected_schema = expected_schema

    def get_pk(self, expected_schema):
        #get pk from schema
        #return col
        pass

    def validate_records(self , df , expected_schema):
        pk_col = self.get_pk(expected_schema)
        #check if 3 functions valid
            #handle_record(df)


    def check_duplicates(df , pk):
        #if no duplicates in pk
            return df
        #else:
            #rows_before , count_duplicates
            #logger(rose_before , count_duplicates , self.file_name)
            #move_rejected_record(duplicated_rows = df)
            #delete from df
            #return > new_df_without_duplicates

    def check_nulls(df):
         pass

    def check_formats(df):
        #loop on df 
            #if row has invalid format
                #logger()
                #rejected_df.append(row)
            #else: 
                #valid_df.append(row) 
        #move_rejected_record(rejected_df , self.file_name)
        return valid_df
    
    def check_value_ranges(df):
         #loop on df
            #if row has invalid range 
                #logger()
                #rejected_df.append(row)
            #else: 
                #valid_df.append(row)
        #move_rejected_record(rejected_df , self.file_name)
        return valid_df
    
    