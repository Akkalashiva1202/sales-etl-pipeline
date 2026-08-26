from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count


# ============================================================
# VERSION 2 - GOLD LAYER
# Silver -> Business Aggregation -> Gold Delta Table
# ============================================================

spark = (
    SparkSession.builder
    .appName("SalesETL-Gold")
    .getOrCreate()
)

print("=== VERSION 2: GOLD LAYER STARTED ===")


# ============================================================
# Table Configuration
# ============================================================

silver_table = "workspace.sales_etl.silver_orders"
gold_table = "workspace.sales_etl.gold_daily_sales"


# ============================================================
# Read Silver Table
# ============================================================

orders_df = spark.table(silver_table)

print("Silver data:")
orders_df.show()


# ============================================================
# VERSION 2 - DAILY SALES AGGREGATION
# ============================================================

daily_sales = (
    orders_df
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        sum("revenue").alias("total_revenue"),
        sum("profit").alias("total_profit")
    )
    .orderBy("order_date")
)


# ============================================================
# Show Gold Data
# ============================================================

print("Gold daily sales:")
daily_sales.show()


# ============================================================
# Write Gold Delta Table
# ============================================================

daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_table)

print(f"Gold table written: {gold_table}")


# ============================================================
# Finish
# ============================================================

spark.stop()

print("=== VERSION 2: GOLD LAYER COMPLETED ===")