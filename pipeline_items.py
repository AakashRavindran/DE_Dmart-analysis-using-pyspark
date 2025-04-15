from pyspark.sql.types import *

customer_csv_path = "<custom_S3_path>"
product_csv_path = "<custom_S3_path>"
sales_csv_path = "<custom_S3_path>"

destination_path = "<custom_S3_path>"


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
