# Import required PySpark modules
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Import logging libraries and configure logging
import logging
import logging.config

# Import helper/config files and modules for data handling
import pipeline_items  # Contains schema definitions and file paths
from resources.credentials import aws_cred  # AWS S3 credentials

# Import custom classes for each pipeline stage
from ingest import Ingestion      # Handles data ingestion
from transform import Transform   # Handles data transformations
from persist import Persist       # Handles persistence (if used)

import sys

# Configure logging using config file
logging.config.fileConfig("resources/configs/logging.conf")
logger = logging.getLogger('my_app')

# Define the main pipeline class
class Pipeline:
    
    # Method to create a Spark session configured for S3 access
    def create_spark_session(self):
        key_id = aws_cred["access_key"]
        session_key = aws_cred["session_key"]
        region = aws_cred["region_name"]

        self.spark = (SparkSession.builder
                      .appName("DataPipeline")
                      .config("spark.jars.packages",
                              "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
                      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                      .config("spark.hadoop.fs.s3a.access.key", key_id)
                      .config("spark.hadoop.fs.s3a.secret.key", session_key)
                      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                      .config("spark.hadoop.fs.s3a.path.style.access", "true")
                      .getOrCreate())

        logger.info("Spark session created with AWS S3 support")

    # Method to run the complete pipeline
    def run_pipeline(self):
        try:
            logger.info("Data Pipeline started")
            logger.info("run_pipeline() method started")

            # Step 1: Ingest data using the Ingestion class
            read_data = Ingestion(self.spark)

            customer_df = read_data.ingest_data(
                pipeline_items.customer_schema,
                pipeline_items.customer_csv_path
            )
            logger.info("Customer data ingested")

            product_df = read_data.ingest_data(
                pipeline_items.product_schema,
                pipeline_items.product_csv_path
            )
            logger.info("Product data ingested")

            sales_df = read_data.ingest_data(
                pipeline_items.sales_schema,
                pipeline_items.sales_csv_path
            )
            logger.info("Sales data ingested")

            # Step 2: Transform data using the Transform class
            process_data = Transform()
            process_data.transform_data(
                self.spark,
                cus_df=customer_df,
                product_df=product_df,
                sales_df=sales_df
            )
            logger.info("Data processing completed")

            # Optional Step 3: Persist data (already handled inside Transform if needed)
            # write_data = Persist()
            # write_data.persist_data()

        except Exception as exp:
            logger.error("Error occurred when running the pipeline >> " + str(exp))
            sys.exit(1)

# Entry point for the script
if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.create_spark_session()  # Initialize Spark session with AWS configs
    pipeline.run_pipeline()          # Run the full ETL pipeline

