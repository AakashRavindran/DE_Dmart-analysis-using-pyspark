# Import necessary PySpark modules and utilities
import pyspark
import pyspark.sql.functions as f
import logging
import logging.config

# Import custom modules for persisting results and configurations
from persist import Persist
import pipeline_items

# Configure logging using the external config file
logging.config.fileConfig("resources/configs/logging.conf")
logger = logging.getLogger('my_app')

# Define the Transform class to perform data transformations and answer business questions
class Transform:

    # Main method to process and transform the input dataframes
    def transform_data(self, spark_session, cus_df, product_df, sales_df):
        logger.info("Data processing started for Dmart Data")

        # Instantiate persistence handler to write output data
        writer = Persist()
        destination_bucket = pipeline_items.destination_path  # Path to save all output results

        # === Data Preparation: Joining Tables ===

        # Join product and sales dataframes on product ID
        join_cond = product_df.pd_product_id == sales_df.sales_product_id
        product_sales_df = product_df.join(sales_df, join_cond, "left")

        # Join customer and sales dataframes on customer ID
        join_cond = cus_df.cus_customer_id == sales_df.sales_customer_id
        customer_sales_df = cus_df.join(sales_df, join_cond, "left")

        # === Business Questions and Transformations ===

        # Q1: Total sales for each product category
        logger.info("Q1. What is the total sales for each product category?")
        q1_df = (product_sales_df.filter("category is not null")
                 .select("category", "sales", "quantity")
                 .groupBy("category")
                 .agg(f.round(f.sum(f.col("Sales") * f.col("Quantity")), 2).alias("total_sales_per_category"))
                 .orderBy("total_sales_per_category", ascending=False))
        writer.persist_data(spark_session, q1_df, destination_bucket, "Q1")

        # Q2: Customer with the highest number of purchases
        logger.info("Q2. Which customer has made the highest number of purchases?")
        q2_df = (customer_sales_df.filter("cus_customer_id is not null")
                 .select(["customer_name", "order_line"])
                 .groupBy("customer_name")
                 .agg(f.count("order_line").alias("no_of_purchases"))
                 .orderBy("no_of_purchases", ascending=False))
        writer.persist_data(spark_session, q2_df, destination_bucket, "Q2")

        # Q3: Average discount on all product sales
        logger.info("Q3. What is the average discount given on sales across all products?")
        q3_df = (product_sales_df.filter("pd_product_id is not null")
                 .groupBy("product_name")
                 .agg(f.round(f.avg("discount"), 2).alias("avg_discount_per_product"))
                 .orderBy("avg_discount_per_product", ascending=False))
        writer.persist_data(spark_session, q3_df, destination_bucket, "Q3")

        # Q4: Number of unique products sold in each region
        logger.info("Q4. How many unique products were sold in each region?")
        q4_df = (customer_sales_df.filter("region is not null")
                 .select(["region", "sales_product_id"])
                 .distinct()
                 .groupBy("region")
                 .agg(f.count("sales_product_id").alias("no_unique_products"))
                 .orderBy("no_unique_products", ascending=False))
        writer.persist_data(spark_session, q4_df, destination_bucket, "Q4")

        # Q5: Total profit generated in each state
        logger.info("Q5. What is the total profit generated in each state?")
        q5_df = (customer_sales_df.filter("state is not null")
                 .select(["state", "profit"])
                 .groupBy("state")
                 .agg(f.round(f.sum("profit"), 2).alias("total_profit_per_state"))
                 .orderBy("total_profit_per_state", ascending=False))
        writer.persist_data(spark_session, q5_df, destination_bucket, "Q5")

        # Q6: Product sub-category with the highest total sales
        logger.info("Q6. Which product sub-category has the highest sales?")
        q6_df = (product_sales_df.filter("sub_category is not null")
                 .select(["sub_category", "sales"])
                 .groupBy("sub_category")
                 .agg(f.round(f.sum("sales"), 2).alias("sales_per_sub_cat"))
                 .orderBy("sales_per_sub_cat", ascending=False)
                 .limit(1))
        writer.persist_data(spark_session, q6_df, destination_bucket, "Q6")

        # Q7: Average age of customers by segment
        logger.info("Q7. What is the average age of customers in each segment?")
        q7_df = (cus_df.filter("segment is not null")
                 .select(["segment", "age"])
                 .groupBy("segment")
                 .agg(f.round(f.avg("age"), 2).alias("avg_age")))
        writer.persist_data(spark_session, q7_df, destination_bucket, "Q7")

        # Q8: Number of orders shipped by shipping mode
        logger.info("Q8. How many orders were shipped in each shipping mode?")
        q8_df = (sales_df.filter("ship_mode is not null")
                 .select(["ship_mode", "order_line"])
                 .groupBy("ship_mode")
                 .agg(f.count("order_line").alias("total_orders")))
        writer.persist_data(spark_session, q8_df, destination_bucket, "Q8")

        # Q9: Total quantity of products sold in each city
        logger.info("Q9. What is the total quantity of products sold in each city?")
        q9_df = (customer_sales_df.filter("city is not null")
                 .select(["city", "quantity"])
                 .groupBy("city")
                 .agg(f.sum("quantity").alias("total_quantity"))
                 .orderBy("total_quantity", ascending=False))
        writer.persist_data(spark_session, q9_df, destination_bucket, "Q9")

        # Q10: Customer segment with the highest total profit
        logger.info("Q10. Which customer segment has the highest profit margin?")
        q10_df = (customer_sales_df.filter("segment is not null")
                  .select(["segment", "profit"])
                  .groupBy("segment")
                  .agg(f.round(f.sum("profit"), 2).alias("profit_margin"))
                  .orderBy("profit_margin", ascending=False)
                  .limit(1))
        writer.persist_data(spark_session, q10_df, destination_bucket, "Q10")
