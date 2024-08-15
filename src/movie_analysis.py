from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg, row_number
from pyspark.sql.window import Window
import logging

class MovieStatisticsCalculator:
    def __init__(self, ratings_df, movies_df):
        self.ratings_df = ratings_df
        self.movies_df = movies_df

    def calculate_statistics(self):
        try:
            logging.info("Calculating movie rating statistics...")
            rating_stats_df = self.ratings_df.groupBy("MovieID").agg(
                spark_max("Rating").alias("MaxRating"),
                spark_min("Rating").alias("MinRating"),
                avg("Rating").alias("AvgRating")
            )
            movies_with_stats_df = self.movies_df.join(rating_stats_df, "MovieID", "left").fillna(0)
            movies_with_stats_df.cache()  # Cache the resulting DataFrame
            logging.info("Movie rating statistics calculated successfully.")
            return movies_with_stats_df
        except Exception as e:
            logging.error(f"Error calculating movie rating statistics: {str(e)}")
            raise e


class TopMoviesPerUser:
    def __init__(self, ratings_df, movies_df):
        self.ratings_df = ratings_df
        self.movies_df = movies_df

    def get_top_3_movies(self):
        try:
            logging.info("Identifying top 3 movies per user...")

            # Remove duplicate movie ratings for the same user, keeping only the highest rating
            window_spec_max = Window.partitionBy("UserID", "MovieID").orderBy(col("Rating").desc(),
                                                                              col("Timestamp").asc())
            highest_rating_df = self.ratings_df.withColumn("rank", row_number().over(window_spec_max)) \
                .filter(col("rank") == 1).drop("rank")

            # Rank movies by rating and timestamp, and keep the top 3 unique movies per user
            window_spec = Window.partitionBy("UserID").orderBy(col("Rating").desc(), col("Timestamp").asc())
            ranked_df = highest_rating_df.withColumn("rank", row_number().over(window_spec))
            top_3_movies_df = ranked_df.filter(col("rank") <= 3)
            top_3_movies_with_titles_df = top_3_movies_df.join(self.movies_df, "MovieID", "left").select("UserID",
                                                                                                         "Title",
                                                                                                         "Rating")
            top_3_movies_with_titles_df.cache()  # Cache the resulting DataFrame
            logging.info("Top 3 movies per user identified successfully.")
            return top_3_movies_with_titles_df
        except Exception as e:
            logging.error(f"Error identifying top 3 movies per user: {str(e)}")
            raise e


class Analyzer:
    def __init__(self, ratings_with_users_df, movies_df=None):
        self.ratings_with_users_df = ratings_with_users_df
        self.movies_df = movies_df

    def analyze_average_ratings(self):
        try:
            logging.info("Analyzing average ratings by demographic groups...")
            avg_rating_by_age_df = self.ratings_with_users_df.groupBy("Age").agg(avg("Rating").alias("AvgRating")).orderBy("Age").cache()
            avg_rating_by_gender_df = self.ratings_with_users_df.groupBy("Gender").agg(avg("Rating").alias("AvgRating")).cache()
            avg_rating_by_occupation_df = self.ratings_with_users_df.groupBy("Occupation").agg(avg("Rating").alias("AvgRating")).orderBy("Occupation").cache()
            logging.info("Average ratings by demographic groups analyzed successfully.")
            return avg_rating_by_age_df, avg_rating_by_gender_df, avg_rating_by_occupation_df
        except Exception as e:
            logging.error(f"Error analyzing average ratings by demographic groups: {str(e)}")
            raise e

    def analyze_preferences(self):
        if not self.movies_df:
            raise ValueError("Movies DataFrame is required for preference analysis.")
        try:
            logging.info("Analyzing movie preferences by demographics...")
            # Top rated movies by age group
            top_movies_by_age_df = self.ratings_with_users_df.groupBy("Age", "MovieID").agg(avg("Rating").alias("AvgRating"))
            top_movies_by_age_df = top_movies_by_age_df.join(self.movies_df, "MovieID", "left").orderBy("Age", "AvgRating", ascending=[True, False]).cache()

            # Top rated movies by gender
            top_movies_by_gender_df = self.ratings_with_users_df.groupBy("Gender", "MovieID").agg(avg("Rating").alias("AvgRating"))
            top_movies_by_gender_df = top_movies_by_gender_df.join(self.movies_df, "MovieID", "left").orderBy("Gender", "AvgRating", ascending=[True, False]).cache()

            # Top rated movies by occupation
            top_movies_by_occupation_df = self.ratings_with_users_df.groupBy("Occupation", "MovieID").agg(avg("Rating").alias("AvgRating"))
            top_movies_by_occupation_df = top_movies_by_occupation_df.join(self.movies_df, "MovieID", "left").orderBy("Occupation", "AvgRating", ascending=[True, False]).cache()

            logging.info("Movie preferences by demographics analyzed successfully.")
            return top_movies_by_age_df, top_movies_by_gender_df, top_movies_by_occupation_df
        except Exception as e:
            logging.error(f"Error analyzing movie preferences by demographics: {str(e)}")
            raise e
