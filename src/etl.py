from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count


# ============================================================
# 1. Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("SalesETL")
    .getOrCreate()
)

print("Spark started successfully")


# ============================================================
# 2. Detect Environment
# ============================================================

try:
    dbutils
    running_in_databricks = True
except NameError:
    running_in_databricks = False


# ============================================================
# 3. Read Input CSV
# ============================================================

if running_in_databricks:

    input_path = (
        "file:/Workspace/Users/"
        "akkalashiva05@gmail.com/"
        "sales-etl-pipeline/"
        "data/orders.csv"
    )

    print("Running in Databricks")

else:

    input_path = "data/orders.csv"

    print("Running locally")


print(f"Reading input file: {input_path}")


orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)


print("Input data:")
orders_df.show()


# ============================================================
# 4. BRONZE LAYER
# ============================================================

bronze_table = "workspace.sales_etl.bronze_orders"

print(f"Writing Bronze table: {bronze_table}")


orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(bronze_table)


print("Bronze table written successfully")


# ============================================================
# 5. SILVER TRANSFORMATION
# ============================================================

orders_transformed = orders_df.withColumn(
    "revenue",
    col("quantity") * col("price")
)


print("Transformed data:")
orders_transformed.show()


# ============================================================
# 6. SILVER LAYER
# ============================================================

silver_table = "workspace.sales_etl.silver_orders"

print(f"Writing Silver table: {silver_table}")


orders_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(silver_table)


print("Silver table written successfully")


# ============================================================
# 7. GOLD AGGREGATION
# ============================================================

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


# ============================================================
# 8. GOLD LAYER
# ============================================================

gold_table = "workspace.sales_etl.gold_daily_sales"

print(f"Writing Gold table: {gold_table}")


daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(gold_table)


print("Gold table written successfully")


# ============================================================
# 9. Stop Spark
# ============================================================

spark.stop()

print("ETL completed successfully")