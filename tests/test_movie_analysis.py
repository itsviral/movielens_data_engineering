import pytest
import os
from pyspark.sql import SparkSession
from src.movie_analysis import TopMoviesPerUser

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("MovieLensTest").getOrCreate()

@pytest.fixture(scope="module")
def data_dir():
    # Get the directory of the current script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, '../data')

def test_top_movies_per_user(spark, data_dir):
    # Sample data to simulate loading
    movies_data = [(1, "Toy Story", "Animation|Children's|Comedy"), (2, "Jumanji", "Adventure|Children's|Fantasy")]
    ratings_data = [(1, 1, 5, 964982703), (1, 2, 4, 964982931), (1, 1, 3, 964983815), (2, 1, 5, 964982703)]

    movies_df = spark.createDataFrame(movies_data, ["MovieID", "Title", "Genres"])
    ratings_df = spark.createDataFrame(ratings_data, ["UserID", "MovieID", "Rating", "Timestamp"])

    top_movies = TopMoviesPerUser(ratings_df, movies_df)
    result_df = top_movies.get_top_3_movies()

    # Check the number of top movies for UserID = 1
    user_1_movies = result_df.filter(result_df.UserID == 1).collect()
    assert len(user_1_movies) == 2, f"Expected 2, but got {len(user_1_movies)}"
    assert user_1_movies[0]["Title"] == "Toy Story", f"Expected 'Toy Story', but got {user_1_movies[0]['Title']}"
    assert user_1_movies[1]["Title"] == "Jumanji", f"Expected 'Jumanji', but got {user_1_movies[1]['Title']}"

    # Check the number of top movies for UserID = 2
    user_2_movies = result_df.filter(result_df.UserID == 2).collect()
    assert len(user_2_movies) == 1, f"Expected 1, but got {len(user_2_movies)}"
    assert user_2_movies[0]["Title"] == "Toy Story", f"Expected 'Toy Story', but got {user_2_movies[0]['Title']}"

if __name__ == "__main__":
    pytest.main()
