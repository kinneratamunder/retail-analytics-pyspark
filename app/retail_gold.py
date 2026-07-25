from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    count,
    avg,
    round
)

# =====================================================
# Create Spark Session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Retail Gold Layer")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =====================================================
# Read Silver Layer
# =====================================================
silver_df = spark.read.parquet(
    "/opt/spark-data/silver/retail_sales_clean.parquet"
)

print("=" * 60)
print("Silver Layer Loaded")
print("=" * 60)

print("Records :", silver_df.count())

# =====================================================
# Create Total Amount
# =====================================================
silver_df = silver_df.withColumn(
    "total_amount",
    round(
        col("quantity") *
        col("unit_price") *
        (1 - col("discount_pct") / 100),
        2
    )
)

# =====================================================
# Daily Sales Metrics
# =====================================================
daily_sales_df = (
    silver_df
    .groupBy("order_date")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("*").alias("total_orders"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
)

daily_sales_df.show(10, truncate=False)

daily_sales_df.write.mode("overwrite").parquet(
    "/opt/spark-data/gold/daily_sales_metrics.parquet"
)

# =====================================================
# Product Category Performance
# =====================================================
category_df = (
    silver_df
    .groupBy("product_category")
    .agg(
        round(sum("total_amount"), 2).alias("category_revenue"),
        sum("quantity").alias("units_sold"),
        count("*").alias("orders")
    )
)

category_df.show(truncate=False)

category_df.write.mode("overwrite").parquet(
    "/opt/spark-data/gold/product_category_performance.parquet"
)

# =====================================================
# City Revenue Metrics
# =====================================================
city_df = (
    silver_df
    .groupBy("city", "state")
    .agg(
        round(sum("total_amount"), 2).alias("city_revenue"),
        count("*").alias("orders"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
)

city_df.show(truncate=False)

city_df.write.mode("overwrite").parquet(
    "/opt/spark-data/gold/city_revenue_metrics.parquet"
)

print("=" * 60)
print("✅ GOLD LAYER CREATED SUCCESSFULLY")
print("=" * 60)

spark.stop()