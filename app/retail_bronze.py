from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType
)

# =====================================================
# Create Spark Session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Retail Bronze Layer")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =====================================================
# Define Explicit Schema
# =====================================================
bronze_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", DateType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("ingestion_date", DateType(), True)
])

# =====================================================
# Read Raw CSV (Bronze Layer)
# =====================================================
bronze_df = (
    spark.read
    .option("header", "true")
    .schema(bronze_schema)
    .csv("/opt/spark-data/raw/retail_sales_raw.csv")
)

# =====================================================
# Validation
# =====================================================
print("=" * 60)
print("Bronze Layer Validation")
print("=" * 60)

print(f"Total Records : {bronze_df.count()}")

print("\nSchema")
bronze_df.printSchema()

print("\nSample Records")
bronze_df.show(10, truncate=False)

# =====================================================
# Write Bronze Layer
# =====================================================
(
    bronze_df.write
    .mode("overwrite")
    .parquet("/opt/spark-data/bronze/retail_sales_bronze.parquet")
)

print("\n======================================")
print("✅ Bronze Layer Created Successfully")
print("======================================")

# =====================================================
# Verify Output
# =====================================================
verify_df = spark.read.parquet(
    "/opt/spark-data/bronze/retail_sales_bronze.parquet"
)

print(f"Verification Count : {verify_df.count()}")

print("\nOutput Schema")
verify_df.printSchema()

print("\nFirst 10 Records")
verify_df.show(10, truncate=False)

# =====================================================
# Stop Spark
# =====================================================
spark.stop()