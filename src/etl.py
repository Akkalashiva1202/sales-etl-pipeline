import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count


# --------------------------------------------------
# 1. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SalesETL")
    .getOrCreate()
)

print("Spark started successfully")


# --------------------------------------------------
# 2. Determine input file path
# --------------------------------------------------

# Databricks sets this environment variable when running
# the code from our Databricks Git folder.
#
# For local execution, we use the normal project path.

DATABRICKS_PATH = (
    "file:/Workspace/Users/akkalashiva05@gmail.com/"
    "sales-etl-pipeline/data/orders.csv"
)

LOCAL_PATH = "data/orders.csv"


if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    input_path = DATABRICKS_PATH
    print("Running in Databricks")
else:
    input_path = LOCAL_PATH
    print("Running locally")


print(f"Reading input file: {input_path}")


# --------------------------------------------------
# 3. Read input data
# --------------------------------------------------

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("Input data:")
orders_df.show()


# --------------------------------------------------
# 4. Transform data
# --------------------------------------------------

orders_transformed = orders_df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

print("Transformed data:")
orders_transformed.show()


# --------------------------------------------------
# 5. Create daily sales summary
# --------------------------------------------------

daily_sales = (
    orders_transformed
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        sum("revenue").alias("total_revenue")
    )
    .orderBy("order_date")
)

print("Daily sales summary:")
daily_sales.show()


# --------------------------------------------------
# 6. Stop Spark
# --------------------------------------------------

spark.stop()

print("ETL completed successfully")