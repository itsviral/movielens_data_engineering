import pytest
from src.data_loader import DataLoader
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestMovieLensAnalysis").getOrCreate()


def test_load_data():
    schema = "MovieID INT, Title STRING, Genres STRING"
    loader = DataLoader('data/movies.dat', schema)  # Corrected path to the root data directory

    df = loader.load_data()

    assert df is not None
    assert df.count() > 0
    assert len(df.columns) == 3  # MovieID, Title, Genres
