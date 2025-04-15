# Import necessary PySpark and logging modules
import pyspark
import logging
import logging.config

# Configure logging using the external config file
logging.config.fileConfig("resources/configs/logging.conf")
logger = logging.getLogger('my_app')


class Persist:
    """
    Persist class responsible for writing transformed data to storage.
    """

    def persist_data(self, spark, dataframe, path, q):
        """
        Writes the given DataFrame to the specified path in CSV format.

        Args:
            spark (SparkSession): Active Spark session.
            dataframe (DataFrame): The DataFrame to persist.
            path (str): Base path where data will be stored.
            q (str): Identifier (e.g., 'Q1', 'Q2') to distinguish output folders.
        """
        logger.info(f"Write operation started for {q}")
        
        # Assign spark session to class instance (not strictly necessary here)
        self.spark = spark

        # Persist the DataFrame as CSV to the specified path with overwrite mode
        dataframe.write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(path + q + "/")  # Creates a folder per question output

        logger.info(f"Write operation completed for {q}")
