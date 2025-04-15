# Import required libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
import logging.config

# Configure logging from external file
logging.config.fileConfig("resources/configs/logging.conf")
logger = logging.getLogger('my_app')

# Define the Ingestion class for reading data into Spark
class Ingestion:

    # Constructor to initialize the class with a Spark session
    def __init__(self, spark_session):
        self.spark = spark_session

    # Method to ingest data from a CSV file using a given schema and path
    def ingest_data(self, data_schema, path):
        logger.info("Data ingestion started")
        try:
            # Read CSV file into a DataFrame using the provided schema
            df = (self.spark.read
                  .format("csv")              # Specify file format
                  .schema(data_schema)        # Apply predefined schema
                  .option("delimiter", ",")   # Set delimiter as comma
                  .option("header", "true")   # Read first row as header
                  .load(path))                # Load data from specified path
        except Exception as exp:
            # Log any errors during data reading
            logger.error("Error occurred during data ingestion >> " + str(exp))
            raise Exception("Ingestion Error")  # Raise custom exception for calling method to handle
        else:
            return df  # Return the loaded DataFrame

        # Optional: Show sample data (currently commented out)
        # df.show(5)
