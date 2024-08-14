import logging
from data_loader import DataLoader
from data_quality import DataQualityChecker, DataCleaner
from movie_analysis import MovieStatisticsCalculator, TopMoviesPerUser, Analyzer
from demographic_analysis import DemographicAnalyzer
from pyspark.sql.functions import col

def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Define schemas
    movies_schema = "MovieID INT, Title STRING, Genres STRING"
    ratings_schema = "UserID INT, MovieID INT, Rating INT, Timestamp LONG"
    users_schema = "UserID INT, Gender STRING, Age INT, Occupation INT, ZipCode STRING"

    # Load data
    movies_loader = DataLoader('../data/movies.dat', movies_schema)
    ratings_loader = DataLoader('../data/ratings.dat', ratings_schema)
    users_loader = DataLoader('../data/users.dat', users_schema)

    movies_df = movies_loader.load_data()
    ratings_df = ratings_loader.load_data()
    users_df = users_loader.load_data()

    # Perform data quality checks
    movies_quality_checker = DataQualityChecker(movies_df, "Movies DataFrame")
    ratings_quality_checker = DataQualityChecker(ratings_df, "Ratings DataFrame")
    users_quality_checker = DataQualityChecker(users_df, "Users DataFrame")

    movies_quality_checker.perform_checks()
    ratings_quality_checker.perform_checks()
    users_quality_checker.perform_checks()

    # Clean data
    ratings_cleaner = DataCleaner(ratings_df, ratings_df.count() - ratings_df.dropDuplicates().count(), invalid_condition=(col("Rating") < 1) | (col("Rating") > 5))
    movies_cleaner = DataCleaner(movies_df, movies_df.count() - movies_df.dropDuplicates().count())
    users_cleaner = DataCleaner(users_df, users_df.count() - users_df.dropDuplicates().count())

    ratings_df = ratings_cleaner.clean_data()
    movies_df = movies_cleaner.clean_data()
    users_df = users_cleaner.clean_data()

    # Calculate movie rating statistics
    movie_stats_calculator = MovieStatisticsCalculator(ratings_df, movies_df)
    movies_with_stats_df = movie_stats_calculator.calculate_statistics()

    # Get top 3 movies per user
    top_movies = TopMoviesPerUser(ratings_df, movies_df)
    top_3_movies_with_titles_df = top_movies.get_top_3_movies()

    # Perform demographic analysis
    demographic_analyzer = DemographicAnalyzer(users_df)
    age_distribution, gender_distribution, occupation_distribution = demographic_analyzer.analyze_demographics()

    # Analyze preferences and average ratings
    ratings_with_users_df = ratings_df.join(users_df, "UserID")
    analyzer = Analyzer(ratings_with_users_df, movies_df)
    top_movies_by_age_df, top_movies_by_gender_df, top_movies_by_occupation_df = analyzer.analyze_preferences()
    avg_rating_by_age_df, avg_rating_by_gender_df, avg_rating_by_occupation_df = analyzer.analyze_average_ratings()

    # Display results (only top 10 records for brevity)
    logging.info("Sample of Movies DataFrame with Rating Statistics:")
    movies_with_stats_df.show(10)

    logging.info("Sample of Top 3 Movies per User DataFrame:")
    top_3_movies_with_titles_df.show(10)

    logging.info("Age Group Distribution:")
    age_distribution.show()

    logging.info("Gender Distribution:")
    gender_distribution.show()

    logging.info("Occupation Distribution:")
    occupation_distribution.show()

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

if __name__ == "__main__":
    main()
