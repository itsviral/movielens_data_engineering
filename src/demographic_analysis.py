import logging

class DemographicAnalyzer:
    def __init__(self, users_df):
        self.users_df = users_df

    def analyze_demographics(self):
        try:
            logging.info("Analyzing user demographics...")
            age_distribution = self.users_df.groupBy("Age").count().orderBy("Age").cache()
            gender_distribution = self.users_df.groupBy("Gender").count().cache()
            occupation_distribution = self.users_df.groupBy("Occupation").count().orderBy("Occupation").cache()
            logging.info("User demographics analyzed successfully.")
            return age_distribution, gender_distribution, occupation_distribution
        except Exception as e:
            logging.error(f"Error analyzing user demographics: {str(e)}")
            raise e
