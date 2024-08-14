from pyspark.sql import SparkSession
import logging

# Initialize Spark session (singleton)
spark = SparkSession.builder.appName("MovieLensAnalysis").getOrCreate()

class DataLoader:
    def __init__(self, filepath, schema):
        self.filepath = filepath
        self.schema = schema
        self.data = None

    def load_data(self):
        try:
            logging.info(f"Loading data from {self.filepath}...")
            self.data = spark.read.option("delimiter", "::").schema(self.schema).csv(self.filepath)
            logging.info(f"Data loaded successfully with {self.data.count()} rows and {len(self.data.columns)} columns.")
        except Exception as e:
            logging.error(f"Error loading data from {self.filepath}: {str(e)}")
            raise e
        return self.data
