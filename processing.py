import pandas as pd
import logging


class Processing:

    def __init__(self, configs, preprocessing):
        self.config = configs
        self.preprocess = preprocessing

    def get_denormalized_df(self, reviews, listing, column):
        logging.debug("In get_denormalized_df")
        merged_df = pd.merge(reviews, listing, on=column, how='outer')
        logging.info("Merged two datasets with outer on column " + column)
        self.check_count_equal(reviews, merged_df)
        return merged_df

    def check_count_equal(self, df1, df2):
        logging.info("checking column counts for equality")
        if df1.shape[0] == df2.shape[0]:
            logging.info("Both DataFrames have the same number of rows." + str(df1.shape[0]))
        else:
            # There must be some listing which don't have
            logging.info("Both DataFrames have different number of rows. "+str(df1.shape[0]) + ' and ' + str(df2.shape[0]))

    def update_missing_columns(self, df, column_list):
        missing_columns = [column for column in column_list if column not in df.columns.to_list()]
        logging.info(df.columns.to_list())
        logging.info(missing_columns)
        df2 = df.reindex(columns=df.columns.tolist() + missing_columns)
        return df2, missing_columns

    def populate_missing_column_values(self, df, missing_columns):
        for column in missing_columns:
            if column+'_apply' in dir(Processing):
                df = eval('self.'+column+'_apply(df)')
            else:
                logging.info('Could not find implimentation for updating missing values of ' + column + ' Create a method in processing.py code with name ' + column + '_apply')
        return df

    def day_apply(self, df):
        df['date'] = pd.to_datetime(df['date'])
        df['day'] = df['date'].dt.day
        return df

    def month_apply(self, df):
        df['date'] = pd.to_datetime(df['date'])
        df['month'] = df['date'].dt.month
        return df

    def year_apply(self, df):
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        return df

    def host_response_rate_apply(self, df):
        # x_host_response_rate = df.loc[df['host_is_superhost'] == False, 'host_response_rate'].()  # .median 1 value_counts().idxmax() 1   # .mean() 0.9658071736828809
        # df['host_response_rate'].fillna(x_host_response_rate, inplace=True)
        df['host_response_rate'].interpolate(method='linear', inplace=True)
        logging.info(" Missing value updated for null rows in host_response_rate ")
        return df

    def create_fact_tables(self, df):
        facts_dict = {}
        fact_tables = self.config.get_fact_tables()
        for fact in fact_tables:
            column_list = self.config.get_fact_table_columns(fact)
            facts_dict[fact] = df[column_list]
        return facts_dict

    def create_dim_tables(self, df):
        dim_dict = {}
        dim_tables = self.config.get_dim_tables()
        for dim in dim_tables:
            column_list = self.config.get_dim_table_columns(dim)
            dim_dict[dim] = df[column_list]
        return dim_dict

    def process_facts_on_keys(self, dict_df):
        dim_model = self.config.dim_model()
        for fact, df in dict_df.items():
            keys = dim_model[fact+'_keys']
            logging.info("keys of fact " + str(keys))
            for column, keytype in keys.items():
                logging.info("Column "+column+" defined as "+keytype)
                if keytype == 'primary_key':
                    # df = df.drop_duplicates(subset=[column], keep='first').dropna(subset=[column])
                    if 'date' in str(df[column].dtypes):
                        logging.info("Converting NaT to NaN for " + column)
                        df = df.loc[pd.to_datetime(df[column], errors='coerce').notna()]
                        # df = df[pd.notnull(df[column])]
                    logging.info("Data type: " + str(df[column].dtypes))
                    df = df.dropna(subset=[column])
                    df = df.drop_duplicates(subset=[column])
                    logging.info("Fact " + fact + " duplicates dropped for column " + column)
                    logging.info("Fact " + fact + " null dropped for column " + column)
                elif keytype == 'forign_key':
                    if 'date' in str(df[column].dtypes):
                        logging.info("Converting NaT to NaN for " + column)
                        df = df.loc[pd.to_datetime(df[column], errors='coerce').notna()]
                        # df = df[pd.notnull(df[column])]
                    df = df.dropna(subset=[column])
                    logging.info("Fact " + fact + " null dropped for column " + column)
            dict_df[fact] = df
        return dict_df

    def process_dims_on_keys(self, dict_df):
        dim_model_struct = self.config.dim_model_struct()
        dim_model = self.config.dim_model()
        for fact in dim_model_struct.facts:
            for dim, df in dict_df.items():
                keys = dim_model[fact+'_keys']
                logging.info("keys of fact " + str(keys))
                for column, keytype in keys.items():
                    logging.info("Fact Column "+column+" defined as "+keytype)
                    if column in df.columns.to_list():
                        logging.info("Column " + column + "Exist in dataframe")
                        if keytype == 'forign_key':
                            if 'date' in str(df[column].dtypes):
                                logging.info("Converting NaT to NaN for " + column)
                                df = df.loc[pd.to_datetime(df[column], errors='coerce').notna()]
                                # df = df[pd.notnull(df[column])]
                            df = df.dropna(subset=[column])
                            df = df.drop_duplicates(subset=[column], keep='first')
                            logging.info("Dim " + dim + " duplicates dropped for column " + column + " as fact " + fact + " has defined it as " + keytype)
                            logging.info("Dim " + dim + " null dropped for column " + column + " as fact " + fact + " has defined it as " + keytype)
                dict_df[dim] = df
        return dict_df

    def dtype_facts(self, dict_df):
        dim_model = self.config.dim_model()
        for fact, df in dict_df.items():
            logging.info("table name: "+fact)
            dtype = dim_model[fact+'_dtype']
            logging.info("table dict: "+str(dtype))
            for column in df.columns:
                logging.info("working on :" + column)
                if 'date' not in dtype[column]:
                    try:
                        logging.info("For fact: " + fact + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column])
                        df[column] = df[column].astype(dtype[column])
                    except Exception as e:
                        logging.info("*"*50)
                        logging.info(e)
                        logging.info("Failed: For fact: " + fact + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column]+ " keeping original type")
                        logging.info("*"*50)
                else:
                    try:
                        logging.info("For fact: " + fact + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column])
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    except Exception as e:
                        logging.info("*"*50)
                        logging.info(e)
                        logging.info("Failed: For fact: " + fact + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column]+ " keeping original type")
                        logging.info("*"*50)
            dict_df[fact] = df
        return dict_df

    def dtype_dims(self, dict_df):
        dim_model = self.config.dim_model()
        for dim, df in dict_df.items():
            dtype = dim_model[dim+'_dtype']
            for column in df.columns:
                if 'date' not in dtype[column]:
                    try:
                        logging.info("For dim: " + dim + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column])
                        df[column] = df[column].astype(dtype[column])
                    except Exception as e:
                        logging.info("*"*50)
                        logging.info(e)
                        logging.info("Failed: For dim: " + dim + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column]+ " keeping original type")
                        logging.info("*"*50)
                else:
                    try:
                        logging.info("For dim: " + dim + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column])
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    except Exception as e:
                        logging.info("*"*50)
                        logging.info(e)
                        logging.info("Failed: For dim: " + dim + " Changing dataype of column: " + column + " from: " + str(df[column].dtypes) + " to " + dtype[column]+ " keeping original type")
                        logging.info("*"*50)
            dict_df[dim] = df
        return dict_df

    def save_dict_df(self, dict_df, path):
        for table, df in dict_df.items():
            self.preprocess.save_df(path+table, df)

    def main(self):

        # reading listing pre_processed dataset
        listing = self.config.get_listing_details()
        logging.info("Reading Preprocessed data from " + listing.destination)
        listing_df = self.preprocess.read_csv(listing.destination)

        # reading reviews pre_processed dataset
        reviews = self.config.get_review_details()
        logging.info("Reading Preprocessed data from " + reviews.destination)
        reviews_df = self.preprocess.read_csv(reviews.destination)

        reviews_listing_df = self.get_denormalized_df(reviews_df, listing_df, self.config.get_column_filter())
        self.preprocess.df_info(reviews_listing_df)
        required_columns_fact_dim = self.config.get_required_columns_fact_dim()

        # Add missing columns and update values as per the requirement
        reviews_listing_df_added_columns, missing_columns = self.update_missing_columns(reviews_listing_df, required_columns_fact_dim)
        logging.info("Following are the new added columns")

        # extending list with inbuild column custom implimentation
        missing_columns.extend(self.config.get_apply_function_on_columns())

        logging.info(reviews_listing_df_added_columns[missing_columns].head(5))
        review_listing_fact_dim = self.populate_missing_column_values(reviews_listing_df_added_columns, missing_columns)
        logging.info(review_listing_fact_dim[missing_columns].head(5))

        facts_df_dict = self.create_fact_tables(review_listing_fact_dim)
        # print(facts_df_dict)
        dims_df_dict = self.create_dim_tables(review_listing_fact_dim)
        # print(dims_df_dict)

        processed_facts_df_dict = self.process_facts_on_keys(facts_df_dict)
        print(processed_facts_df_dict)
        processed_dims_df_dict = self.process_dims_on_keys(dims_df_dict)
        print(processed_dims_df_dict)
        # review_listing_fact_dim = self.update_dtype(review_listing_fact_dim)
        dtype_processed_facts_df_dict = self.dtype_facts(processed_facts_df_dict)
        print(dtype_processed_facts_df_dict)

        dtype_processed_dims_df_dict = self.dtype_facts(processed_dims_df_dict)
        print(dtype_processed_dims_df_dict)

        self.save_dict_df(dtype_processed_facts_df_dict, self.config.get_fact_save())
        self.save_dict_df(dtype_processed_dims_df_dict, self.config.get_dim_save())
