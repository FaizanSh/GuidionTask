import pandas as pd
import logging


class Data_Quality_Checks:

    def __init__(self, configs, preprocess):
        self.config = configs
        self.preprocess = preprocess
        self.failure = """
                        ------------------------------------
                            Data Quality Check Failure
                        ------------------------------------
                        Check Details: {name}
                        Table:		   {df}
                        ------------------------------------
                        """

    # Completeness
    def check_dictioanry_column(self, dict_df):
        dim_mdl = self.config.dim_model()
        for table, df in dict_df.items():
            columns = dim_mdl[table+'_columns']
            if set(columns) == set(df.columns.to_list()):
                logging.info("Dictionary columns exist in table "+table)
            else:
                logging.info(self.failure.format(name='check_dictioanry_column', df=table))

    def check_dictioanry_column_order(self, dict_df):
        dim_mdl = self.config.dim_model()
        for table, df in dict_df.items():
            columns = dim_mdl[table+'_columns']
            if columns == df.columns.to_list():
                logging.info("Dictionary column order is maintained in "+table)
            else:
                logging.info(self.failure.format(name='check_dictioanry_column_order', df=table))

    # Datatypes
    def check_dictioanry_types(self):
        # cant impliment as reading from csv files
        pass

    # Incorrect values
    def check_null_in_key_column(self, dict_df):
        dim_model_struct = self.config.dim_model_struct()
        dim_model = self.config.dim_model()
        for fact in dim_model_struct.facts:
            for table, df in dict_df.items():
                keys = dim_model[fact+'_keys']
                logging.info("keys in dict " + str(keys))
                for column, keytype in keys.items():
                    logging.info("Table: "+table+" Column "+column+" defined as "+keytype)
                    if column in df.columns.to_list():
                        logging.info("Column " + column + " Exist in table " + table)
                        if keytype == 'forign_key' or keytype == 'primary_key':
                            null_check = df[column].isna()
                            if null_check.any():
                                logging.info(self.failure.format(name="check_null_in_key_column", df=table))
                            else:
                                logging.info("Table " + table + " does not have nulls in " + column + " as fact " + fact + " has defined it as " + keytype)

    # Unexpected data
    def check_duplicate_in_key_column(self, dict_df):
        dim_model_struct = self.config.dim_model_struct()
        dim_model = self.config.dim_model()
        for fact in dim_model_struct.facts:
            for table, df in dict_df.items():
                keys = dim_model[fact+'_keys']
                logging.info("keys in dict " + str(keys))
                for column, keytype in keys.items():
                    logging.info("Table: "+table+" Column "+column+" defined as "+keytype)
                    if column in df.columns.to_list():
                        logging.info("Column " + column + " Exist in table " + table)
                        if (keytype == 'primary_key') and (table in dim_model_struct.facts):
                            duplicate_check = df[column].duplicated()
                            if duplicate_check.any():
                                logging.info(self.failure.format(name="check_duplicate_in_key_column", df=table))
                            else:
                                logging.info("Table " + table + " does not have duplicates in " + column + " as fact " + fact + " has defined it as " + keytype)
                        elif (keytype == 'primary_key') and (table not in dim_model_struct.facts):
                            duplicate_check = df[column].duplicated()
                            if duplicate_check.any():
                                logging.info(self.failure.format(name="check_duplicate_in_key_column", df=table))
                            else:
                                logging.info("Table " + table + " does not have duplicates in " + column + " as fact " + fact + " has defined it as " + keytype)
                        elif (keytype == 'forign_key') and (table in dim_model_struct.facts):
                            pass
                        elif (keytype == 'forign_key') and (table not in dim_model_struct.facts):
                            duplicate_check = df[column].duplicated()
                            if duplicate_check.any():
                                logging.info(self.failure.format(name="check_duplicate_in_key_column", df=table))
                            else:
                                logging.info("Table " + table + " does not have duplicates in " + column + " as fact " + fact + " has defined it as " + keytype)

    # Range Check
    def check_negative_in_key_column(self, dict_df):
        dim_model_struct = self.config.dim_model_struct()
        dim_model = self.config.dim_model()
        for fact in dim_model_struct.facts:
            for table, df in dict_df.items():
                dtype = dim_model[table+'_dtype']
                keys = dim_model[fact+'_keys']
                logging.info("keys in dict " + str(keys))
                for column, keytype in keys.items():
                    logging.info("Table: "+table+" Column "+column+" defined as "+keytype)
                    if column in df.columns.to_list():
                        logging.info("Column " + column + " Exist in table " + table)
                        if ((keytype == 'forign_key') or (keytype == 'primary_key')) and (('int' in dtype[column]) or ('float' in dtype[column])):
                            range_check = df[df[column] < 0]
                            if range_check.any().any():
                                logging.info(self.failure.format(name="check_negative_in_key_column", df=table))
                            else:
                                logging.info("Table " + table + " does not have negatives in " + column + " as fact " + fact + " has defined it as " + keytype)

    # External Validity
    def check_primary_key_validity(self, dict_df, source_data):
        dim_model_struct = self.config.dim_model_struct()
        dim_model = self.config.dim_model()
        # for all fact tables
        for fact in dim_model_struct.facts:
            # for all source
            for source in source_data:
                # all tables/df in dict - compare it with each source and run against each fact's primary/forign key
                for table, df in dict_df.items():
                    keys = dim_model[fact+'_keys']
                    logging.info("keys in dict " + str(keys))
                    for column, keytype in keys.items():
                        logging.info("Table: "+table+" Column "+column+" defined as "+keytype)
                        # check if fact column exist in the dict dataframe
                        if column in df.columns.to_list():
                            logging.info("Column " + column + " Exist in table " + table)
                            # check for any key and also if that column exist in source dataframe
                            if (keytype == 'primary_key' or keytype == 'forign_key') and (column in source.columns.to_list()):
                                if set(df[column]) != set(source[column]):
                                    logging.info(self.failure.format(name="check_primary_key_validity", df=table))
                                else:
                                    logging.info("Table " + table + " primary column " + column + " are equal from the source, as fact " + fact + " has defined it as " + keytype)

    def read_facts(self):
        dim_struct = self.config.dim_model_struct()
        fact_df_dict = {}

        for fact in dim_struct.facts:
            logging.info("Reading processed data for table " + fact)
            fact_df_dict[fact] = self.preprocess.read_csv(dim_struct.fact_save+fact+'.csv')
        return fact_df_dict

    def read_dims(self):
        dim_struct = self.config.dim_model_struct()
        dim_df_dict = {}

        for dim in dim_struct.dimentions:
            logging.info("Reading processed data for table " + dim)
            dim_df_dict[dim] = self.preprocess.read_csv(dim_struct.dim_save+dim+'.csv')
        return dim_df_dict

    def main(self):
        # reading listing pre_processed dataset
        fact_df_dict = self.read_facts()
        dim_df_dict = self.read_dims()

        self.check_dictioanry_column(fact_df_dict)
        self.check_dictioanry_column(dim_df_dict)

        self.check_dictioanry_column_order(fact_df_dict)
        self.check_dictioanry_column_order(dim_df_dict)

        self.check_null_in_key_column(fact_df_dict)
        self.check_null_in_key_column(dim_df_dict)

        self.check_duplicate_in_key_column(fact_df_dict)
        self.check_duplicate_in_key_column(dim_df_dict)

        self.check_negative_in_key_column(fact_df_dict)
        self.check_negative_in_key_column(dim_df_dict)

        source_data = []
        # reading listing pre_processed dataset
        listing = self.config.get_listing_details()
        logging.info("Reading Preprocessed data from " + listing.destination)
        listing_df = self.preprocess.read_csv(listing.destination)
        source_data.append(listing_df)
        # reading reviews pre_processed dataset
        reviews = self.config.get_review_details()
        logging.info("Reading Preprocessed data from " + reviews.destination)
        reviews_df = self.preprocess.read_csv(reviews.destination)
        source_data.append(reviews_df)
        self.check_primary_key_validity(dim_df_dict, source_data)