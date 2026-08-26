from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count


# 1. Create Spark session
spark = (
    SparkSession.builder
    .appName("SalesETL")
    .getOrCreate()
)

print("Spark started successfully")


# 2. Read sample data
orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)

print("Input data:")
orders_df.show()


# 3. Transform data
orders_transformed = orders_df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

print("Transformed data:")
orders_transformed.show()


# 4. Create daily sales summary
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


# 5. Stop Spark
spark.stop()

print("ETL completed successfully")