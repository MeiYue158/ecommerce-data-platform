"""
SCD Type 2 — dim_customer

Builds a slowly changing dimension with:
  - Customer segment: Bronze/Silver/Gold/Platinum based on order spend
  - City changes for ~15% of multi-order customers (synthetic)
  - effective_from, effective_to, is_current

Every customer starts as Bronze at account creation (first order - 30 days).
After their first order, their segment is assigned based on total spend.
This creates real version transitions for ~70% of customers.

Usage:
    spark-submit build_scd2_customer.py <ingestion_date>
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


SILVER_BASE = "s3a://ecommerce-data/silver"
GOLD_BASE = "s3a://ecommerce-data/gold"


def assign_segment(spend_col):
    return (
        F.when(F.col(spend_col) >= 300, "Platinum")
        .when(F.col(spend_col) >= 150, "Gold")
        .when(F.col(spend_col) >= 50, "Silver")
        .otherwise("Bronze")
    )


def create_spark_session():
    return (
        SparkSession.builder
        .appName("build_scd2_customer")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    if len(sys.argv) < 2:
        print("Usage: build_scd2_customer.py <ingestion_date>")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  SCD TYPE 2 — dim_customer — {ingestion_date}")
    print(f"{'='*60}\n")

    customers = spark.read.parquet(f"{SILVER_BASE}/customers/ingestion_date={ingestion_date}")
    orders = spark.read.parquet(f"{SILVER_BASE}/orders/ingestion_date={ingestion_date}")
    order_items = spark.read.parquet(f"{SILVER_BASE}/order_items/ingestion_date={ingestion_date}")
    geography = spark.read.parquet(f"{SILVER_BASE}/geography/ingestion_date={ingestion_date}")

    # ── Customer lifetime metrics ──
    order_spend = order_items.groupBy("order_id").agg(F.sum("price").alias("order_total"))
    customer_metrics = (
        orders
        .join(order_spend, "order_id", "inner")
        .groupBy("customer_id")
        .agg(
            F.sum("order_total").alias("total_spend"),
            F.count("*").alias("order_count"),
            F.min("order_purchase_timestamp").alias("first_order_date"),
            F.max("order_purchase_timestamp").alias("last_order_date"),
        )
        .withColumn("final_segment", assign_segment("total_spend"))
    )

    # ── Synthetic city changes for ~15% of customers ──
    city_changers = (
        customer_metrics
        .withColumn("hash_val", F.abs(F.hash("customer_id")) % 100)
        .filter(F.col("hash_val") < 15)
        .select("customer_id", "first_order_date")
    )

    other_cities = (
        customers
        .select("customer_state", "customer_city").distinct()
        .withColumn(
            "alt_city",
            F.lead("customer_city").over(
                Window.partitionBy("customer_state").orderBy("customer_city")
            ),
        )
        .filter(F.col("alt_city").isNotNull())
    )

    city_change_info = (
        city_changers
        .join(customers.select("customer_id", "customer_city", "customer_state"), "customer_id")
        .join(other_cities, ["customer_state", "customer_city"], "left")
        .filter(F.col("alt_city").isNotNull())
        # City change happens 30 days after first order
        .withColumn("city_change_date", F.date_add(F.col("first_order_date").cast("date"), 30))
        .select("customer_id", "alt_city", "city_change_date")
    )

    # ── Build version records ──
    base = customers.join(customer_metrics, "customer_id", "inner")

    # VERSION 1: Initial state — Bronze, original city
    # Effective from 30 days before first order (account creation)
    v1 = (
        base
        .withColumn("customer_segment", F.lit("Bronze"))
        .withColumn("cumulative_spend", F.lit(0.0))
        .withColumn("effective_from", F.date_sub(F.col("first_order_date").cast("date"), 30))
        .withColumn("version", F.lit(1))
    )

    # VERSION 2: After first purchase — actual segment, original city
    # Only for customers whose segment is NOT Bronze (would be no change)
    v2 = (
        base
        .filter(F.col("final_segment") != "Bronze")
        .withColumn("customer_segment", F.col("final_segment"))
        .withColumn("cumulative_spend", F.col("total_spend"))
        .withColumn("effective_from", F.col("first_order_date").cast("date"))
        .withColumn("version", F.lit(2))
    )

    # VERSION 3: City change — same segment, new city
    # Only for city changers
    v3 = (
        base
        .join(city_change_info, "customer_id", "inner")
        .withColumn("customer_segment", F.col("final_segment"))
        .withColumn("cumulative_spend", F.col("total_spend"))
        .withColumn("customer_city", F.col("alt_city"))
        .withColumn("effective_from", F.col("city_change_date"))
        .withColumn("version", F.lit(3))
        .drop("alt_city", "city_change_date")
    )

    # ── Union all versions ──
    common_cols = [
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state",
        "customer_segment", "cumulative_spend", "effective_from", "version",
    ]
    all_versions = (
        v1.select(common_cols)
        .unionByName(v2.select(common_cols))
        .unionByName(v3.select(common_cols))
    )

    # ── Build SCD2 fields ──
    scd_window = Window.partitionBy("customer_id").orderBy("effective_from", "version")

    scd2 = (
        all_versions
        .withColumn(
            "effective_to",
            F.coalesce(
                F.date_sub(F.lead("effective_from").over(scd_window), 1),
                F.lit("9999-12-31").cast("date"),
            ),
        )
        .withColumn(
            "is_current",
            F.col("effective_to") == F.lit("9999-12-31").cast("date"),
        )
        .drop("version")
    )

    # Join geography
    scd2 = scd2.join(
        geography.select("zip_prefix", "latitude", "longitude"),
        scd2["customer_zip_code_prefix"] == geography["zip_prefix"],
        "left",
    ).drop("zip_prefix")

    # Surrogate key
    sk_window = Window.orderBy("customer_id", "effective_from")
    scd2 = (
        scd2
        .withColumn("customer_sk", F.row_number().over(sk_window))
        .select(
            "customer_sk", "customer_id", "customer_unique_id",
            F.col("customer_zip_code_prefix").alias("zip_prefix"),
            F.col("customer_city").alias("city"),
            F.col("customer_state").alias("state"),
            "latitude", "longitude",
            "customer_segment", "cumulative_spend",
            "effective_from", "effective_to", "is_current",
        )
    )

    # ── Write ──
    output_path = f"{GOLD_BASE}/dim_customer_scd2"
    scd2.write.mode("overwrite").parquet(output_path)

    total = scd2.count()
    unique = scd2.select("customer_id").distinct().count()
    multi = scd2.groupBy("customer_id").count().filter(F.col("count") > 1).count()
    current = scd2.filter(F.col("is_current")).count()

    print(f"  Total rows (versions):   {total:>10,}")
    print(f"  Unique customers:        {unique:>10,}")
    print(f"  Customers w/ >1 version: {multi:>10,}")
    print(f"  Current records:         {current:>10,}")

    print(f"\n  --- Segment Distribution (current) ---")
    scd2.filter(F.col("is_current")).groupBy("customer_segment").agg(
        F.count("*").alias("customers"),
    ).orderBy("customer_segment").show(truncate=False)

    print(f"  --- Version Count Distribution ---")
    scd2.groupBy("customer_id").count().groupBy("count").agg(
        F.count("*").alias("customers"),
    ).withColumnRenamed("count", "versions").orderBy("versions").show(truncate=False)

    print(f"  --- Sample: 3-Version Customer (segment + city change) ---")
    sample = scd2.groupBy("customer_id").count().filter(F.col("count") == 3).orderBy("customer_id").first()
    if sample:
        scd2.filter(F.col("customer_id") == sample["customer_id"]).orderBy(
            "effective_from"
        ).select(
            "customer_sk", "city", "customer_segment",
            "cumulative_spend", "effective_from", "effective_to", "is_current",
        ).show(truncate=30)

    # ── Integrity checks ──
    print(f"  --- SCD Integrity Checks ---")
    multi_current = scd2.filter(F.col("is_current")).groupBy("customer_id").count().filter(F.col("count") > 1).count()
    no_current = unique - scd2.filter(F.col("is_current")).select("customer_id").distinct().count()
    overlaps = scd2.alias("a").join(
        scd2.alias("b"),
        (F.col("a.customer_id") == F.col("b.customer_id"))
        & (F.col("a.customer_sk") != F.col("b.customer_sk"))
        & (F.col("a.effective_from") <= F.col("b.effective_to"))
        & (F.col("a.effective_to") >= F.col("b.effective_from")),
    ).count()
    print(f"  >1 current per customer:  {multi_current}")
    print(f"  0 current records:        {no_current}")
    print(f"  Overlapping date ranges:  {overlaps}")

    spark.stop()
    print(f"\n  Output: {output_path}")
    print("  Done.")


if __name__ == "__main__":
    main()
