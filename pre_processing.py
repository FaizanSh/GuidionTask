import pandas as pd
import logging
import glob
import os
from utils.config_reader import Config_Reader


class Pre_Processing:

    def __init__(self, configs):
        self.config = configs

    # Perform operations on dataframe here
    @staticmethod
    def save_chunk(chunk_count, df):
        logging.debug('Executing process_chunk funtion')
        df.drop_duplicates(inplace=True)
        df.to_csv(f'temp\\chunk_{chunk_count}.csv', index=False)
        logging.info(f'temp\\chunk_{chunk_count}.csv File Successfully Saved on Disk')

    def save_df(self, name, df):
        logging.info(f'Saving file {name}')
        if not name.endswith('.csv'):
            name += '.csv'
        df.to_csv(f'{name}', index=False)
        logging.info(f'{name}File Successfully Saved on Disk')

    # get basic info of a dataframe
    def df_info(self, df):
        pd.set_option('display.max_columns', None)

        logging.info('Calling head() on dataframe')
        logging.info(df.head())

        logging.info('Calling info() on dataframe')
        logging.info(df.info())

        logging.info('Calling describe() on dataframe')
        logging.info(df.describe())

    def column_as_custom(self, df):
        logging.debug('Updating column to lowercase and replacing spaces with underscores')
        df.columns = [col.replace(' ', '_').lower() for col in df.columns]
        logging.info("Replaced spaces with underscore in column name in la listing")
        logging.debug(df.head())
        return df

    def filter_on_column(self, df1, df2, column):
        # df1[df1[column].isin(df2[column])]
        logging.debug(df1.head())
        logging.debug(df2.head())
        logging.debug(column)

        merged_df = pd.merge(df1, df2[column], on=column, how='inner')
        return merged_df

    def read_csv(self, filename, encoding='ISO-8859-1', delimiter=','):
        logging.info("Reading from file "+filename)
        dataframe = pd.read_csv(filename, encoding=encoding, delimiter=delimiter)
        logging.debug(dataframe)
        return dataframe

    def read_csv_in_chunks(self, file='None', chunk=10**3, encode='ISO-8859-1', sep=','):
        logging.debug('In Function apply_function_file_chunks')
        chunk_iter = pd.read_csv(file, chunksize=chunk, encoding=encode, delimiter=sep)
        logging.debug('Read file as Iterator')
        return chunk_iter

    @staticmethod
    def combine_chunks():
        all_chunks = glob.glob("temp\\chunk_*.csv")
        la_airbnb_reviews = pd.concat([pd.read_csv(f) for f in all_chunks], ignore_index=True)
        logging.info('All Chunks Combined')
        return la_airbnb_reviews

    @staticmethod
    def remove_cunks():
        logging.info('Dropping all temp\\ folder files')
        for f in glob.glob("temp\\chunk_*.csv"):
            os.remove(f)
        logging.info('All Chunks in temp\\ folder has dropped')

    def main(self):
        logging.info("Execution Starts for Data Fimiliarity and Distribution")

        # setup data file path
        listing_dataset = self.config.get_listing_details()
        la_listings = listing_dataset.source  # 'datasets\\LA_Listings\\LA_Listings.csv'  # encoding='ISO-8859-1' # non-UTF-8 # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xd1 in position 24: invalid continuation byte
        logging.info("Dataset path identificaiton"+str(la_listings))

        review_dataset = self.config.get_review_details()
        airbnb_reviews = review_dataset.source  # 'datasets\\airbnb-reviews\\airbnb-reviews.csv'
        logging.info("Dataset path identificaiton"+str(airbnb_reviews))

        column_filter = self.config.get_column_filter()
        filter_column = column_filter  # 'listing_id'
        logging.info("Specifying filering column")

        # Read data
        df_la_listings = self.read_csv(filename=la_listings)
        logging.info("Imported Smalled Dataset")
        df_la_listings = self.column_as_custom(df_la_listings)
        df_la_listings.drop_duplicates(inplace=True)
        logging.info("Dropping duplicates in la listing")

        # Read Chunks and apply fuction
        chunk_iter = self.read_csv_in_chunks(file=airbnb_reviews, sep=review_dataset.delemeter, chunk=review_dataset.chunk)
        logging.info("Imported larger dataset in chunks")

        for chunk_count, chunk in enumerate(chunk_iter):
            logging.info('Processing Chunk '+str(chunk_count))
            self.column_as_custom(chunk)
            logging.debug('Applying Row Filter on chunks')
            update_chunk = self.filter_on_column(chunk, df_la_listings, filter_column)
            self.save_chunk(chunk_count, update_chunk)
        logging.info('All Chunks Created')

        df_all_chunks = self.combine_chunks()

        la_airbnb_reviews = df_all_chunks.drop_duplicates(inplace=False)
        logging.info('Dropping duplicates on all chunks')

        # understanding datasets
        logging.info('Understanding la_listing')
        self.df_info(df_la_listings)

        # understanding datasets
        logging.info('Understanding reviews')
        self.df_info(la_airbnb_reviews)

        self.save_df(listing_dataset.destination, la_airbnb_reviews)  # "pre_processed\\la_airbnb-reviews"
        self.save_df(review_dataset.destination, df_la_listings)  # "pre_processed\\la_listings"

        self.remove_cunks()
