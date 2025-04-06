# Databricks notebook source
# import the required spark functions and types

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------


#Creating the required Schemas ( Renaming columns and setting the data types)

customer_schema = StructType([
    StructField("cus_customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("segment", StringType()),
    StructField("age", IntegerType()),
    StructField("country", StringType()),
     StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("postal_code", StringType()),
    StructField("region", StringType(), True)
]
)

product_schema = StructType(
    [
        StructField("pd_product_id",StringType()),
        StructField("category",StringType()),
        StructField("sub_category",StringType()),
        StructField("product_name",StringType())
    ]
)


sales_schema = StructType(
    [
        StructField("order_line",IntegerType()),
        StructField("order_id",StringType()),
        StructField("order_date",DateType()),
        StructField("ship_date",DateType()),
        StructField("ship_mode",StringType()),
        StructField("sales_customer_id",StringType()),
        StructField("sales_product_id",StringType()),
        StructField("Sales",DoubleType()),
        StructField("Quantity",IntegerType()),
        StructField("Discount",DoubleType()),
        StructField("Profit",DoubleType())
    ]
)



# COMMAND ----------

# Load The data from the local path into Spark Dataframes


customer_df = product_df = spark.read \
                   .format("csv") \
                   .schema(customer_schema) \
                   .option("header","true") \
                   .option("delimiter", ",") \
                   .load("dbfs:/FileStore/sample_data/Customer.csv")


product_df = spark.read \
                   .format("csv") \
                   .schema(product_schema) \
                   .option("header","true") \
                   .load("dbfs:/FileStore/sample_data/Product.csv")

sales_df = spark.read \
                   .format("csv") \
                   .schema(sales_schema) \
                   .option("header","true") \
                   .load("dbfs:/FileStore/sample_data/Sales.csv")


                                 

# COMMAND ----------

customer_df.show()

# COMMAND ----------

product_df.show(5)

# COMMAND ----------

sales_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q1.What is the total sales for each product category?"
# MAGIC

# COMMAND ----------

# Joining Product and Sales Data Frames

join_expr_pd_sales = product_df.pd_product_id == sales_df.sales_product_id

product_sales_df = product_df.join(sales_df, join_expr_pd_sales, "left")

# COMMAND ----------


product_sales_df.filter("category is not null") \
    .select("category", "sales") \
    .groupBy("category") \
    .agg(round(sum(col("sales")),2).alias("total_sales_per_product")) \
    .orderBy("total_sales_per_product", ascending=False) \
    .show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Q2.Which customer has made the highest number of purchases?
# MAGIC

# COMMAND ----------

# Joining Customer and Sales Data Frames

join_expr_cus_sales = customer_df.cus_customer_id == sales_df.sales_customer_id


customer_sales_df = customer_df.join(sales_df, join_expr_cus_sales, "left")

# COMMAND ----------

(
    customer_sales_df.filter("cus_customer_id is not null")
    .groupBy(col("customer_name"))
    .agg(count(col("order_line")).alias("total_purchases"))
    .orderBy("total_purchases",ascending = False)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q3.What is the average discount given on sales across all products?

# COMMAND ----------

(
    product_sales_df.filter("pd_product_id is not null")
    .groupBy("product_name")
    .agg(round(avg(col("discount")),2).alias("avg_discount_per_product"))
    .orderBy("avg_discount_per_product", ascending = False)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q4.How many unique products were sold in each region?
# MAGIC

# COMMAND ----------

(customer_sales_df.filter("region is not null")
.select(col("region"), col("sales_product_id"))
.distinct()
.groupBy(col("region"))
.agg(count("sales_product_id").alias("no_unique_products"))
.orderBy("no_unique_products", ascending = False)
.show())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q5.What is the total profit generated in each state?
# MAGIC

# COMMAND ----------

(
    customer_sales_df.filter("state is not null")
    .select(col("state"),col("profit"))
    .groupBy(col("state"))
    .agg(round(sum(col("profit")),2).alias("total_profit_per_state"))
    .orderBy("total_profit_per_state",ascending = False)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q6.Which product sub-category has the highest sales?
# MAGIC

# COMMAND ----------

(
    product_sales_df.filter("sub_category is not null")
    .select(col("sub_category"), col("sales"))
    .groupBy(col("sub_category"))
    .agg(round(sum(col("sales")),2).alias("sales_per_sub_cat"))
    .orderBy("sales_per_sub_cat",ascending = False)
    .limit(1)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q7.What is the average age of customers in each segment?
# MAGIC

# COMMAND ----------

customer_df.filter("segment is not null") \
.select(col("segment"),col("age")) \
.groupBy(col("segment")) \
.agg(round(avg(col("age")),2).alias("avg_age")) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q8.How many orders were shipped in each shipping mode?

# COMMAND ----------

sales_df.filter("ship_mode is not null") \
.select(col("ship_mode"),col("order_line")) \
.groupBy(col("ship_mode")) \
.agg(count(col("order_line")).alias("total_orders")) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q9.What is the total quantity of products sold in each city?
# MAGIC

# COMMAND ----------

customer_sales_df.filter("city is not null") \
.select(col("city"),col("quantity")) \
.groupBy(col("city")) \
.agg(sum(col("quantity")).alias("total_quantity")) \
.orderBy("total_quantity",ascending = False) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q10.Which customer segment has the highest profit margin?
# MAGIC

# COMMAND ----------

customer_sales_df.filter("segment is not null") \
.select(col("segment"),col("profit")) \
.groupBy(col("segment")) \
.agg(round(sum(col("profit")),2).alias("profit_margin")) \
.orderBy("profit_margin",ascending = False) \
.limit(1) \
.show()

# COMMAND ----------

