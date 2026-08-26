from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


# ============================================================
# VERSION 2 - SILVER LAYER
# Bronze -> Transformation -> Silver Delta Table
# ============================================================

spark = (
    SparkSession.builder
    .appName("SalesETL-Silver")
    .getOrCreate()
)

print("=== VERSION 2: SILVER LAYER STARTED ===")


# ============================================================
# Table Configuration
# ============================================================

bronze_table = "workspace.sales_etl.bronze_orders"
silver_table = "workspace.sales_etl.silver_orders"


# ============================================================
# Read Bronze Table
# ============================================================

orders_df = spark.table(bronze_table)

print("Bronze data:")
orders_df.show()


# ============================================================
# Calculate Revenue
# ============================================================

orders_transformed = orders_df.withColumn(
    "revenue",
    col("quantity") * col("price")
)


# ============================================================
# VERSION 2 - FUTURE SCHEMA CHANGE
# Handle Profit Column
# ============================================================

if "profit" not in orders_transformed.columns:

    print("Profit column not found.")
    print("Adding profit as NULL.")

    orders_transformed = orders_transformed.withColumn(
        "profit",
        lit(None).cast("double")
    )

else:

    print("Profit column found.")


# ============================================================
# Show Silver Data
# ============================================================

print("Silver data:")
orders_transformed.show()


# ============================================================
# Write Silver Delta Table
# ============================================================

orders_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print(f"Silver table written: {silver_table}")


# ============================================================
# Finish
# ============================================================

spark.stop()

print("=== VERSION 2: SILVER LAYER COMPLETED ===")