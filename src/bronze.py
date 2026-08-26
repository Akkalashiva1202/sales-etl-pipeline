from pyspark.sql import SparkSession


# ============================================================
# VERSION 2 - BRONZE LAYER
# Raw CSV -> Bronze Delta Table
# ============================================================

spark = (
    SparkSession.builder
    .appName("SalesETL-Bronze")
    .getOrCreate()
)

print("=== VERSION 2: BRONZE LAYER STARTED ===")


# ============================================================
# Input Configuration
# ============================================================

input_path = (
    "file:/Workspace/Users/"
    "akkalashiva05@gmail.com/"
    "sales-etl-pipeline/"
    "data/orders.csv"
)

bronze_table = "workspace.sales_etl.bronze_orders"

print(f"Reading input: {input_path}")


# ============================================================
# Read Raw CSV
# ============================================================

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("Raw input:")
orders_df.show()


# ============================================================
# Write Bronze Delta Table
# ============================================================

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(bronze_table)

print(f"Bronze table written: {bronze_table}")


# ============================================================
# Finish
# ============================================================

spark.stop()

print("=== VERSION 2: BRONZE LAYER COMPLETED ===")