from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    upper,
    trim
)

# =====================================================
# Create Spark Session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Retail Silver Layer")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =====================================================
# Read Bronze Layer
# =====================================================
bronze_df = spark.read.parquet(
    "/opt/spark-data/bronze/retail_sales_bronze.parquet"
)

print("=" * 60)
print("Bronze Layer Loaded")
print("=" * 60)

print("Bronze Count :", bronze_df.count())
bronze_df.printSchema()

# =====================================================
# Duplicate Check
# =====================================================
print("\nDuplicate Transaction IDs")

bronze_df.groupBy("transaction_id") \
    .count() \
    .filter(col("count") > 1) \
    .show(10, truncate=False)

silver_df = bronze_df.dropDuplicates(["transaction_id"])

print("After Removing Duplicates :", silver_df.count())

# =====================================================
# Invalid Ship Dates
# =====================================================
print("\nInvalid Ship Dates")

silver_df.filter(
    col("ship_date") < col("order_date")
).show(10, truncate=False)

silver_df = silver_df.withColumn(
    "ship_date",
    when(
        col("ship_date") < col("order_date"),
        None
    ).otherwise(col("ship_date"))
)

# =====================================================
# Quantity Cleaning
# =====================================================
print("\nInvalid Quantities")

silver_df.filter(
    col("quantity") <= 0
).show(10, truncate=False)

silver_df = silver_df.filter(
    col("quantity") > 0
)

# =====================================================
# Unit Price Cleaning
# =====================================================
print("\nInvalid Prices")

silver_df.filter(
    col("unit_price") <= 0
).show(10, truncate=False)

silver_df = silver_df.withColumn(
    "unit_price",
    when(
        col("unit_price") <= 0,
        None
    ).otherwise(col("unit_price"))
)

# =====================================================
# Discount Cleaning
# =====================================================
print("\nInvalid Discounts")

silver_df.filter(
    (col("discount_pct") < 0) |
    (col("discount_pct") > 100)
).show(10, truncate=False)

silver_df = silver_df.withColumn(
    "discount_pct",
    when(
        (col("discount_pct") < 0) |
        (col("discount_pct") > 100),
        None
    ).otherwise(col("discount_pct"))
)

# =====================================================
# Customer Age Cleaning
# =====================================================
print("\nInvalid Customer Ages")

silver_df.filter(
    (col("customer_age") < 15) |
    (col("customer_age") > 100)
).show(10, truncate=False)

silver_df = silver_df.withColumn(
    "customer_age",
    when(
        (col("customer_age") < 15) |
        (col("customer_age") > 100),
        None
    ).otherwise(col("customer_age"))
)

# =====================================================
# Standardize Gender
# =====================================================
print("\nGender Before Cleaning")

silver_df.groupBy("gender").count().show()

silver_df = silver_df.withColumn(
    "gender",
    when(
        upper(trim(col("gender"))) == "MALE",
        "M"
    )
    .when(
        upper(trim(col("gender"))) == "FEMALE",
        "F"
    )
    .when(
        col("gender").isin("M", "F"),
        col("gender")
    )
    .otherwise(None)
)

print("\nGender After Cleaning")

silver_df.groupBy("gender").count().show()

# =====================================================
# Standardize Payment Type
# =====================================================
print("\nInvalid Payment Types")

silver_df.filter(
    ~col("payment_type").isin(
        "Card",
        "UPI",
        "COD"
    )
).show(10, truncate=False)

silver_df = silver_df.withColumn(
    "payment_type",
    when(
        col("payment_type").isin(
            "Card",
            "UPI",
            "COD"
        ),
        col("payment_type")
    ).otherwise(None)
)

# =====================================================
# Final Validation
# =====================================================
print("=" * 60)
print("Silver Layer Validation")
print("=" * 60)

print("Final Record Count :", silver_df.count())

silver_df.printSchema()

print("\nSample Records")

silver_df.show(10, truncate=False)

# =====================================================
# Write Silver Layer
# =====================================================
(
    silver_df
    .repartition(8)
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/silver/retail_sales_clean.parquet")
)

print("\n===================================")
print("✅ Silver Layer Created Successfully")
print("===================================")

# =====================================================
# Verify Output
# =====================================================
verify_df = spark.read.parquet(
    "/opt/spark-data/silver/retail_sales_clean.parquet"
)

print("Verification Count :", verify_df.count())

verify_df.printSchema()

verify_df.show(10, truncate=False)

# =====================================================
# Stop Spark
# =====================================================
spark.stop()