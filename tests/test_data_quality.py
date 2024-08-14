import pytest
from src.data_quality import DataQualityChecker, DataCleaner
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestMovieLensAnalysis").getOrCreate()


def test_perform_checks():
    data = [(1, "Toy Story (1995)", "Animation|Children's|Comedy"),
            (2, "Jumanji (1995)", "Adventure|Children's|Fantasy"),
            (2, "Jumanji (1995)", "Adventure|Children's|Fantasy")]

    df = spark.createDataFrame(data, ["MovieID", "Title", "Genres"])

    checker = DataQualityChecker(df, "Movies DataFrame")
    missing_values, duplicate_entries = checker.perform_checks()

    assert duplicate_entries == 1
    assert all(value == 0 for value in missing_values.values())


def test_clean_data():
    data = [(1, "Toy Story (1995)", "Animation|Children's|Comedy"),
            (2, "Jumanji (1995)", "Adventure|Children's|Fantasy"),
            (2, "Jumanji (1995)", "Adventure|Children's|Fantasy")]

    df = spark.createDataFrame(data, ["MovieID", "Title", "Genres"])
    checker = DataQualityChecker(df, "Movies DataFrame")
    _, duplicate_entries = checker.perform_checks()

    cleaner = DataCleaner(df, duplicate_entries)
    cleaned_df = cleaner.clean_data()

    assert cleaned_df.count() == 2  # Duplicate removed
