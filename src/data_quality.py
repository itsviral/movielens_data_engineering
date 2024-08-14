import logging
from pyspark.sql.functions import col


class DataQualityChecker:
    def __init__(self, df, df_name):
        self.df = df
        self.df_name = df_name

    def perform_checks(self):
        try:
            logging.info(f"Performing data quality checks on {self.df_name}...")
            missing_values = {col: self.df.filter(self.df[col].isNull()).count() for col in self.df.columns}
            duplicate_entries = self.df.count() - self.df.dropDuplicates().count()
            logging.info(f"Missing values in {self.df_name}: {missing_values}")
            logging.info(f"Duplicate entries in {self.df_name}: {duplicate_entries}")
            return missing_values, duplicate_entries
        except Exception as e:
            logging.error(f"Error during data quality checks on {self.df_name}: {str(e)}")
            raise e


class DataCleaner:
    def __init__(self, df, duplicate_entries, invalid_condition=None):
        self.df = df
        self.duplicate_entries = duplicate_entries
        self.invalid_condition = invalid_condition

    def clean_data(self):
        try:
            if self.duplicate_entries > 0:
                self.df = self.df.dropDuplicates()
                logging.info(f"Removed {self.duplicate_entries} duplicate rows.")

            if self.invalid_condition is not None:
                self.df = self.df.filter(~self.invalid_condition)
                logging.info(f"Removed invalid rows.")

            self.df.cache()  # Cache the cleaned DataFrame for reuse
            return self.df
        except Exception as e:
            logging.error(f"Error during data cleaning: {str(e)}")
            raise e
