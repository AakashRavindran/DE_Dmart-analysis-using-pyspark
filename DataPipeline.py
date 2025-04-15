# Import necessary libraries and modules
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
import logging.config
import pipeline_items  # Custom module containing schema definitions and file paths
from ingest import Ingestion  # Custom module to handle data ingestion
from transform import Transform  # Custom module to handle data transformation
from persist import Persist  # Custom module to handle data persistence (currently unused)
import sys

# Configure logging using external config file
logging.config.fileConfig("resources/configs/logging.conf")
logger = logging.getLogger('my_app')

# Define the main Pipeline class
class Pipeline:
    # Method to create a Spark session
    def create_spark_session(self):
        self.spark = (SparkSession.builder
                      .appName("DataPipeline")
                      .getOrCreate())
        logger.info("Spark session created")

    # Method to run the complete pipeline
    def run_pipeline(self):
        try:
            logger.info("Data Pipeline started")
            logger.info("run_pipeline() method started")

            # Step 1: Ingest data using the Ingestion class
            read_data = Ingestion(self.spark)

            # Ingest customer data using schema and file path from pipeline_items
            customer_df = read_data.ingest_data(pipeline_items.customer_schema, pipeline_items.customer_csv_path)
            logger.info("Customer data ingested")

            # Ingest product data
            product_df = read_data.ingest_data(pipeline_items.product_schema, pipeline_items.product_csv_path)
            logger.info("Product data ingested")

            # Ingest sales data
            sales_df = read_data.ingest_data(pipeline_items.sales_schema, pipeline_items.sales_csv_path)
            logger.info("Sales data ingested")

            # Step 2: Transform data using the Transform class
            process_data = Transform()
            process_data.transform_data(self.spark, cus_df=customer_df, product_df=product_df, sales_df=sales_df)
            logger.info("Data processing completed")

        except Exception as exp:
            # Log any exception and exit the script with status 1
            logger.error("Error occurred when running the pipeline >> " + str(exp))
            sys.exit(1)

        return  # Exit the method gracefully

        # Optional Step 3: Persist transformed data (currently commented out)
        # write_data = Persist()
        # write_data.persist_data()

# Entry point of the script
if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.create_spark_session()  # Create Spark session
    pipeline.run_pipeline()  # Execute the pipeline
