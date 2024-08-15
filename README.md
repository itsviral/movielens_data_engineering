```markdown
# MovieLens Data Engineering Project

This project processes the MovieLens dataset using Apache Spark. It reads in movies and ratings data, calculates rating statistics, identifies top movies per user, and performs demographic analysis. The results are stored in Parquet format.

## Project Structure

```
movielens_data_engineering/
│
├── src/
│   ├── __init__.py
│   ├── data_loader.py
│   ├── data_quality.py
│   ├── movie_analysis.py
│   ├── demographic_analysis.py
│   └── main.py
│
├── tests/
│   ├── __init__.py
│   ├── test_data_loader.py
│   ├── test_data_quality.py
│   ├── test_movie_analysis.py
│   └── test_demographic_analysis.py
│
├── data/
│   ├── movies.dat
│   ├── ratings.dat
│   └── users.dat
│
├── logs/
│   └── (generated log files)
│
├── conftest.py
├── pytest.ini
├── log4j.properties
├── spark_submit.sh
├── requirements.txt
└── README.md
```

## Prerequisites

- Python: Ensure Python 3.7+ is installed.
- Apache Spark**: Ensure Apache Spark 2.4+ is installed.
- Java: Ensure Java 8 or higher is installed.

## Setup Instructions

1. Clone the Repository:

   ```bash
   git clone https://github.com/itsviral/movielens_data_engineering.git
   cd movielens_data_engineering
   ```

2. **Create and Activate a Virtual Environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. **Install Required Packages:**

   ```bash
   pip install -r requirements.txt
   ```

## Running the Project

### Option 1: Running `main.py` Directly

You can run the `main.py` script directly to execute the job:

```bash
python src/main.py
```

### Option 2: Using `spark-submit`

Alternatively, you can use the `spark-submit.sh` script to run the Spark job:

```bash
sh spark_submit.sh
```

This will execute the Spark job with the specified configurations, including logging settings defined in `log4j.properties`.

## Running Tests

The project includes unit tests to verify functionality. To run the tests:

1. **Navigate to the Project Root Directory:**

   Ensure you are in the root directory of the project where `pytest.ini` is located.

2. **Run Pytest:**

   ```bash
   pytest
   ```

This command will execute all tests in the `tests/` directory and provide a summary of the results.

## Log Management

Logs are stored in the `logs/` directory within the project. The logs are configured to rotate daily. The logging configuration is defined in the `log4j.properties` file.

## Project Details

- **Data Loading**: The data is loaded from `movies.dat`, `ratings.dat`, and `users.dat` files into Spark DataFrames.
- **Data Quality Checks**: The data is checked for missing values and duplicates.
- **Rating Statistics**: The job calculates max, min, and average ratings for each movie.
- **Top Movies per User**: The job identifies each user’s top 3 movies based on ratings.
- **Demographic Analysis**: The job analyzes user demographics based on age, gender, and occupation.
- **Data Storage**: All results are stored in Parquet format for efficient storage and retrieval.

## Additional Information can be found in report.pdf
