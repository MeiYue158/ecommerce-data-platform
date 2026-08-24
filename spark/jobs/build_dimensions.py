"""
Build Dimension Tables

Creates star-schema dimension tables from Silver layer data.
Assigns deterministic surrogate keys via row_number() over natural keys.

Dimensions:
    dim_date              — date spine (2016-01-01 to 2018-12-31)
    dim_customer          — one row per customer_id
    dim_product           — one row per product_id
    dim_seller            — one row per seller_id
    dim_geography         — one row per zip_prefix
    dim_payment_type      — one row per payment_type

Usage:
    spark-submit build_dimensions.py <ingestion_date> [dimension_name|all]
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


SILVER_BASE = "s3a://ecommerce-data/silver"
GOLD_BASE = "s3a://ecommerce-data/gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("build_dimensions")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_silver(spark, table, ingestion_date):
    return spark.read.parquet(f"{SILVER_BASE}/{table}/ingestion_date={ingestion_date}")


def write_gold(df, name):
    path = f"{GOLD_BASE}/{name}"
    df.write.mode("overwrite").parquet(path)
    count = df.count()
    print(f"  [{name}] {count:,} rows -> {path}")
    return count


# ──────────────────────────────────────────────────────────────
# dim_date — conformed date dimension (generated, not from source)
# ──────────────────────────────────────────────────────────────

def build_dim_date(spark, ingestion_date):
    """Generate date spine from 2016-01-01 to 2018-12-31 (1096 days)."""
    df = spark.range(0, 1096)

    df = (
        df
        .withColumn("date", F.date_add(F.lit("2016-01-01").cast("date"), F.col("id").cast("int")))
        .withColumn("date_sk", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False))
        .withColumn("year_month", F.date_format("date", "yyyy-MM"))
        .withColumn("year_quarter", F.concat(
            F.year("date").cast("string"), F.lit("-Q"), F.quarter("date").cast("string")
        ))
        .drop("id")
        .select(
            "date_sk", "date", "year", "quarter", "month", "month_name",
            "week", "day", "day_of_week", "day_name", "is_weekend",
            "year_month", "year_quarter",
        )
    )

    return write_gold(df, "dim_date")


# ──────────────────────────────────────────────────────────────
# dim_customer
# ──────────────────────────────────────────────────────────────

def build_dim_customer(spark, ingestion_date):
    """
    Grain: one row per customer_id.
    Natural key: customer_id
    Business key: customer_unique_id (same person can have multiple customer_ids)
    """
    customers = read_silver(spark, "customers", ingestion_date)
    geography = read_silver(spark, "geography", ingestion_date)

    # Join with geography for lat/lng
    df = customers.join(
        geography.select("zip_prefix", "latitude", "longitude"),
        customers["customer_zip_code_prefix"] == geography["zip_prefix"],
        "left",
    ).drop("zip_prefix")

    # Assign surrogate key (deterministic via natural key ordering)
    window = Window.orderBy("customer_id")
    df = (
        df
        .withColumn("customer_sk", F.row_number().over(window))
        .select(
            "customer_sk",
            "customer_id",
            "customer_unique_id",
            F.col("customer_zip_code_prefix").alias("zip_prefix"),
            F.col("customer_city").alias("city"),
            F.col("customer_state").alias("state"),
            "latitude",
            "longitude",
        )
    )

    return write_gold(df, "dim_customer")


# ──────────────────────────────────────────────────────────────
# dim_product
# ──────────────────────────────────────────────────────────────

def build_dim_product(spark, ingestion_date):
    """
    Grain: one row per product_id.
    Includes English category name from Silver (already translated).
    """
    products = read_silver(spark, "products", ingestion_date)

    window = Window.orderBy("product_id")
    df = (
        products
        .withColumn("product_sk", F.row_number().over(window))
        .select(
            "product_sk",
            "product_id",
            F.col("product_category").alias("category"),
            F.col("product_category_name").alias("category_original"),
            F.col("product_weight_g").alias("weight_g"),
            F.col("product_length_cm").alias("length_cm"),
            F.col("product_height_cm").alias("height_cm"),
            F.col("product_width_cm").alias("width_cm"),
            F.col("product_photos_qty").alias("photos_qty"),
            F.col("product_name_lenght").alias("name_length"),
            F.col("product_description_lenght").alias("description_length"),
        )
    )

    return write_gold(df, "dim_product")


# ──────────────────────────────────────────────────────────────
# dim_seller
# ──────────────────────────────────────────────────────────────

def build_dim_seller(spark, ingestion_date):
    """Grain: one row per seller_id."""
    sellers = read_silver(spark, "sellers", ingestion_date)
    geography = read_silver(spark, "geography", ingestion_date)

    df = sellers.join(
        geography.select("zip_prefix", "latitude", "longitude"),
        sellers["seller_zip_code_prefix"] == geography["zip_prefix"],
        "left",
    ).drop("zip_prefix")

    window = Window.orderBy("seller_id")
    df = (
        df
        .withColumn("seller_sk", F.row_number().over(window))
        .select(
            "seller_sk",
            "seller_id",
            F.col("seller_zip_code_prefix").alias("zip_prefix"),
            F.col("seller_city").alias("city"),
            F.col("seller_state").alias("state"),
            "latitude",
            "longitude",
        )
    )

    return write_gold(df, "dim_seller")


# ──────────────────────────────────────────────────────────────
# dim_geography
# ──────────────────────────────────────────────────────────────

def build_dim_geography(spark, ingestion_date):
    """Grain: one row per zip_prefix (from resolved geography)."""
    geo = read_silver(spark, "geography", ingestion_date)

    window = Window.orderBy("zip_prefix")
    df = (
        geo
        .withColumn("geography_sk", F.row_number().over(window))
        .select(
            "geography_sk",
            "zip_prefix",
            "city",
            "state",
            "latitude",
            "longitude",
            "sample_count",
        )
    )

    return write_gold(df, "dim_geography")


# ──────────────────────────────────────────────────────────────
# dim_payment_type — small/junk dimension
# ──────────────────────────────────────────────────────────────

def build_dim_payment_type(spark, ingestion_date):
    """Grain: one row per payment_type. Small dimension (5 rows)."""
    payments = read_silver(spark, "order_payments", ingestion_date)

    df = payments.select("payment_type").distinct()

    window = Window.orderBy("payment_type")
    df = df.withColumn("payment_type_sk", F.row_number().over(window))
    df = df.select("payment_type_sk", "payment_type")

    return write_gold(df, "dim_payment_type")


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

DIMENSIONS = {
    "dim_date": build_dim_date,
    "dim_customer": build_dim_customer,
    "dim_product": build_dim_product,
    "dim_seller": build_dim_seller,
    "dim_geography": build_dim_geography,
    "dim_payment_type": build_dim_payment_type,
}


def main():
    if len(sys.argv) < 2:
        print("Usage: build_dimensions.py <ingestion_date> [dim_name|all]")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else "all"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  BUILD DIMENSIONS — {ingestion_date}")
    print(f"{'='*60}\n")

    if target == "all":
        for name, fn in DIMENSIONS.items():
            fn(spark, ingestion_date)
    else:
        if target not in DIMENSIONS:
            print(f"Unknown dimension: {target}")
            print(f"Available: {list(DIMENSIONS.keys())}")
            sys.exit(1)
        DIMENSIONS[target](spark, ingestion_date)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
