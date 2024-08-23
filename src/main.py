import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from data_loader import DataLoader
from data_quality import DataQualityChecker, DataCleaner
from movie_analysis import MovieStatisticsCalculator, TopMoviesPerUser, Analyzer
from demographic_analysis import DemographicAnalyzer

def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create a Spark session
    spark = SparkSession.builder.appName("MovieLensDataEngineering").getOrCreate()
    #spark.sparkContext.setLogLevel("INFO")

    # Define schemas
    movies_schema = "MovieID INT, Title STRING, Genres STRING"
    ratings_schema = "UserID INT, MovieID INT, Rating INT, Timestamp LONG"
    users_schema = "UserID INT, Gender STRING, Age INT, Occupation INT, ZipCode STRING"

    # Get the directory of the current script
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct absolute paths dynamically
    data_dir = os.path.join(base_dir, '../data')
    output_dir = os.path.join(base_dir, '../output')

    # Load data
    # -----------------------------------------------------------------------------
    # Q1 : Read in movies.dat and ratings.dat to spark dataframes.
    logging.info("Loading data...")
    movies_loader = DataLoader(os.path.join(data_dir, 'movies.dat'), movies_schema)
    ratings_loader = DataLoader(os.path.join(data_dir, 'ratings.dat'), ratings_schema)
    users_loader = DataLoader(os.path.join(data_dir, 'users.dat'), users_schema)

    movies_df = movies_loader.load_data()
    ratings_df = ratings_loader.load_data()
    users_df = users_loader.load_data()

    # Perform data quality checks
    logging.info("Performing data quality checks...")
    movies_quality_checker = DataQualityChecker(movies_df, "Movies DataFrame")
    ratings_quality_checker = DataQualityChecker(ratings_df, "Ratings DataFrame")
    users_quality_checker = DataQualityChecker(users_df, "Users DataFrame")

    movies_quality_checker.perform_checks()
    ratings_quality_checker.perform_checks()
    users_quality_checker.perform_checks()

    # Clean data
    logging.info("Cleaning data...")
    ratings_cleaner = DataCleaner(
        ratings_df,
        ratings_df.count() - ratings_df.dropDuplicates().count(),
        invalid_condition=(col("Rating") < 1) | (col("Rating") > 5)
    )
    movies_cleaner = DataCleaner(movies_df, movies_df.count() - movies_df.dropDuplicates().count())
    users_cleaner = DataCleaner(users_df, users_df.count() - users_df.dropDuplicates().count())

    ratings_df = ratings_cleaner.clean_data()
    movies_df = movies_cleaner.clean_data()
    users_df = users_cleaner.clean_data()

    # Calculate movie rating statistics
    # -----------------------------------------------------------------------------------------------
    #  Q2 : Creates a new dataframe, which contains the movies data and 3 new columns max, min and
    #       average rating for that movie from the ratings data.
    logging.info("Calculating movie rating statistics...")
    movie_stats_calculator = MovieStatisticsCalculator(ratings_df, movies_df)
    movies_with_stats_df = movie_stats_calculator.calculate_statistics()
    # Display results (only top 10 records for brevity)
    movies_with_stats_df.orderBy(desc('AvgRating')).show(10)

    # Get top 3 movies per user
    #-----------------------------------------------------------------------------------------------
    # Q3 : Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies
    #      based on their rating.
    logging.info("Identifying top 3 movies per user...")
    top_movies = TopMoviesPerUser(ratings_df, movies_df)
    top_3_movies_with_titles_df = top_movies.get_top_3_movies()
    top_3_movies_with_titles_df.show(10)


    # Additional Data analysis to understand on user data
    #-------------------------------------------------------------------------------------------
    # Perform demographic analysis
    logging.info("Performing demographic analysis...")
    demographic_analyzer = DemographicAnalyzer(users_df)
    age_distribution, gender_distribution, occupation_distribution = demographic_analyzer.analyze_demographics()
    logging.info("Age Group Distribution:")
    age_distribution.show()

    logging.info("Gender Distribution:")
    gender_distribution.show()

    logging.info("Occupation Distribution:")
    occupation_distribution.show()

    # Analyze preferences and average ratings
    logging.info("Analyzing preferences and average ratings...")
    ratings_with_users_df = ratings_df.join(users_df, "UserID")
    analyzer = Analyzer(ratings_with_users_df, movies_df)
    top_movies_by_age_df, top_movies_by_gender_df, top_movies_by_occupation_df = analyzer.analyze_preferences()
    avg_rating_by_age_df, avg_rating_by_gender_df, avg_rating_by_occupation_df = analyzer.analyze_average_ratings()

    logging.info("Sample of Top Rated Movies by Age Group:")
    top_movies_by_age_df.show(10)

    logging.info("Sample of Top Rated Movies by Gender:")
    top_movies_by_gender_df.show(10)

    logging.info("Sample of Top Rated Movies by Occupation:")
    top_movies_by_occupation_df.show(10)

    logging.info("Sample of Average Rating by Age Group:")
    avg_rating_by_age_df.show()

    logging.info("Sample of Average Rating by Gender:")
    avg_rating_by_gender_df.show()

    logging.info("Sample of Average Rating by Occupation:")
    avg_rating_by_occupation_df.show()

    #-----------------------------------------------------------------------------------------------
    # Q4 : Write out the original and new dataframes in an efficient format of your choice.
    # Repartition DataFrames to 1 partition and save them to disk
    logging.info("Saving dataframes to disk with a single partition...")
    movies_df.repartition(1).write.parquet(os.path.join(output_dir, 'movies.parquet'), mode="overwrite")
    ratings_df.repartition(1).write.parquet(os.path.join(output_dir, 'ratings.parquet'), mode="overwrite")
    users_df.repartition(1).write.parquet(os.path.join(output_dir, 'users.parquet'), mode="overwrite")
    movies_with_stats_df.repartition(1).write.parquet(os.path.join(output_dir, 'movies_with_stats.parquet'), mode="overwrite")
    top_3_movies_with_titles_df.repartition(1).write.parquet(os.path.join(output_dir, 'top_3_movies_per_user.parquet'), mode="overwrite")

    logging.info("All dataframes saved successfully in Parquet format with a single partition.")

if __name__ == "__main__":
    main()
