# Databricks Notebook: Sales Data Analysis with PySpark

This Databricks notebook is designed to load, analyze, and visualize sales-related data from CSV files. It performs several queries on customer, product, and sales datasets to provide insights on various business questions.

## Prerequisites

- Databricks Community Cloud or equivalent Spark environment
- PySpark library
- CSV files containing the data:  
  - `Customer.csv`  
  - `Product.csv`  
  - `Sales.csv`  

Make sure the CSV files are available in the appropriate location on your Databricks File System (DBFS).

## Setup

1. Import required Spark functions and types:
   - `from pyspark.sql.functions import *`
   - `from pyspark.sql.types import *`

2. Define the schemas for the `Customer`, `Product`, and `Sales` datasets to properly structure the data:
   - `customer_schema`
   - `product_schema`
   - `sales_schema`

3. Load the data from CSV files into Spark DataFrames:
   - `customer_df`
   - `product_df`
   - `sales_df`

4. Show sample data from each DataFrame for verification.

## Analysis Queries


### 1. Total Sales for Each Product Category

- **Query**: Calculate the total sales for each product category.
- **Method**: Perform a left join between `Product` and `Sales` datasets. Group by category and aggregate total sales.


product_sales_df.filter("category is not null") \
    .select("category", "sales") \
    .groupBy("category") \
    .agg(round(sum(col("sales")),2).alias("total_sales_per_product")) \
    .orderBy("total_sales_per_product", ascending=False) \
    .show()
### 2. Customer with the Highest Number of Purchases
Query: Identify the customer who has made the highest number of purchases.

Method: Join Customer and Sales datasets. Group by customer name and count the number of orders.

customer_sales_df.filter("cus_customer_id is not null") \
    .groupBy(col("customer_name")) \
    .agg(count(col("order_line")).alias("total_purchases")) \
    .orderBy("total_purchases", ascending=False) \
    .show()

    
3. Average Discount Given Across All Products
Query: Calculate the average discount given on sales for each product.

Method: Group by product name and calculate the average discount.

product_sales_df.filter("pd_product_id is not null") \
    .groupBy("product_name") \
    .agg(round(avg(col("discount")),2).alias("avg_discount_per_product")) \
    .orderBy("avg_discount_per_product", ascending=False) \
    .show()

    
4. Number of Unique Products Sold in Each Region
Query: Find the number of unique products sold in each region.

Method: Join Customer and Sales datasets, group by region, and count distinct product IDs.

customer_sales_df.filter("region is not null") \
    .select(col("region"), col("sales_product_id")) \
    .distinct() \
    .groupBy(col("region")) \
    .agg(count("sales_product_id").alias("no_unique_products")) \
    .orderBy("no_unique_products", ascending=False) \
    .show()
    
    
5. Total Profit Generated in Each State
Query: Calculate the total profit generated in each state.

Method: Group by state and sum the profit.

customer_sales_df.filter("state is not null") \
    .select(col("state"), col("profit")) \
    .groupBy(col("state")) \
    .agg(round(sum(col("profit")),2).alias("total_profit_per_state")) \
    .orderBy("total_profit_per_state", ascending=False) \
    .show()
    
    
6. Product Sub-Category with the Highest Sales
Query: Find the product sub-category with the highest sales.

Method: Group by sub-category and sum the sales.


product_sales_df.filter("sub_category is not null") \
    .select(col("sub_category"), col("sales")) \
    .groupBy(col("sub_category")) \
    .agg(round(sum(col("sales")),2).alias("sales_per_sub_cat")) \
    .orderBy("sales_per_sub_cat", ascending=False) \
    .limit(1) \
    .show()

    
7. Average Age of Customers in Each Segment
Query: Calculate the average age of customers in each segment.

Method: Group by segment and calculate the average age.

customer_df.filter("segment is not null") \
    .select(col("segment"), col("age")) \
    .groupBy(col("segment")) \
    .agg(round(avg(col("age")),2).alias("avg_age")) \
    .show()

    
8. Number of Orders Shipped in Each Shipping Mode
Query: Find how many orders were shipped in each shipping mode.

Method: Group by ship mode and count the number of orders.

sales_df.filter("ship_mode is not null") \
    .select(col("ship_mode"), col("order_line")) \
    .groupBy(col("ship_mode")) \
    .agg(count(col("order_line")).alias("total_orders")) \
    .show()

    
9. Total Quantity of Products Sold in Each City
Query: Calculate the total quantity of products sold in each city.

Method: Group by city and sum the quantity.

customer_sales_df.filter("city is not null") \
    .select(col("city"), col("quantity")) \
    .groupBy(col("city")) \
    .agg(sum(col("quantity")).alias("total_quantity")) \
    .orderBy("total_quantity", ascending=False) \
    .show()

    
10. Customer Segment with the Highest Profit Margin
Query: Identify the customer segment with the highest profit margin.

Method: Group by segment and sum the profit.


customer_sales_df.filter("segment is not null") \
    .select(col("segment"), col("profit")) \
    .groupBy(col("segment")) \
    .agg(round(sum(col("profit")),2).alias("profit_margin")) \
    .orderBy("profit_margin", ascending=False) \
    .limit(1) \
    .show()

    
Conclusion
This notebook helps in understanding customer behavior, sales trends, and product performance through a variety of analytical queries. By combining customer, product, and sales data, businesses can derive valuable insights for better decision-making.
