import pytest
from src.demographic_analysis import DemographicAnalyzer
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestMovieLensAnalysis").getOrCreate()


def test_analyze_demographics():
    data = [(1, "M", 25, 1, "12345"),
            (2, "F", 35, 3, "67890"),
            (3, "M", 25, 3, "54321")]

    df = spark.createDataFrame(data, ["UserID", "Gender", "Age", "Occupation", "ZipCode"])
    analyzer = DemographicAnalyzer(df)

    age_distribution, gender_distribution, occupation_distribution = analyzer.analyze_demographics()

    assert age_distribution.count() == 2
    assert gender_distribution.count() == 2
    assert occupation_distribution.count() == 2
