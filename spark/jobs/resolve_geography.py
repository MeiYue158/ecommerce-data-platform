"""
Phase 6: Geolocation Resolution

The raw geolocation table has ~1M rows with multiple coordinates per ZIP prefix.
This job creates a canonical geography mapping:
  - One row per zip_prefix
  - Average latitude/longitude
  - Most frequent (canonical) city and state
  - Record count per zip for confidence

Demonstrates: large-table aggregation, deduplication, normalization, Spark shuffle.

Usage:
    spark-submit resolve_geography.py <ingestion_date>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


BRONZE_BASE = "s3a://ecommerce-data/bronze"
SILVER_BASE = "s3a://ecommerce-data/silver"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("resolve_geography")
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
        print("Usage: resolve_geography.py <ingestion_date>")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read raw geolocation ──
    raw_path = f"{BRONZE_BASE}/geolocation/ingestion_date={ingestion_date}"
    raw = spark.read.parquet(raw_path)
    data_cols = [c for c in raw.columns if not c.startswith("_")]
    raw = raw.select(data_cols)

    raw_count = raw.count()
    zip_count = raw.select("geolocation_zip_code_prefix").distinct().count()

    print(f"\n{'='*60}")
    print(f"  GEOLOCATION RESOLUTION — {ingestion_date}")
    print(f"{'='*60}")
    print(f"\n  Raw records:    {raw_count:>10,}")
    print(f"  Unique ZIPs:    {zip_count:>10,}")
    print(f"  Avg rows/ZIP:   {raw_count / zip_count:>10.1f}")

    # ── Normalize city names and state codes ──
    raw = (
        raw
        .withColumn("geolocation_city", F.initcap(F.trim(F.col("geolocation_city"))))
        .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
        .withColumn(
            "geolocation_zip_code_prefix",
            F.lpad(F.col("geolocation_zip_code_prefix").cast("string"), 5, "0"),
        )
    )

    # ── Aggregate coordinates: average lat/lng per ZIP ──
    coords = raw.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("latitude"),
        F.avg("geolocation_lng").alias("longitude"),
        F.count("*").alias("sample_count"),
        F.stddev("geolocation_lat").alias("lat_stddev"),
        F.stddev("geolocation_lng").alias("lng_stddev"),
    )

    # ── Resolve canonical city/state: most frequent per ZIP ──
    # Count occurrences of each (zip, city, state) combination
    city_counts = raw.groupBy(
        "geolocation_zip_code_prefix",
        "geolocation_city",
        "geolocation_state",
    ).agg(F.count("*").alias("freq"))

    # Pick the most frequent city/state per ZIP
    window = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.desc("freq"))
    canonical = (
        city_counts
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank", "freq")
        .withColumnRenamed("geolocation_city", "city")
        .withColumnRenamed("geolocation_state", "state")
    )

    # ── Join coordinates with canonical city/state ──
    geography = coords.join(canonical, on="geolocation_zip_code_prefix", how="inner")

    # Rename for clean schema
    geography = (
        geography
        .withColumnRenamed("geolocation_zip_code_prefix", "zip_prefix")
        .select(
            "zip_prefix", "city", "state",
            F.round("latitude", 6).alias("latitude"),
            F.round("longitude", 6).alias("longitude"),
            "sample_count",
            F.round("lat_stddev", 6).alias("lat_stddev"),
            F.round("lng_stddev", 6).alias("lng_stddev"),
        )
    )

    final_count = geography.count()

    # ── Write to Silver ──
    output_path = f"{SILVER_BASE}/geography/ingestion_date={ingestion_date}"
    geography.write.mode("overwrite").parquet(output_path)

    # ── Summary stats ──
    print(f"\n  Output records: {final_count:>10,}")
    print(f"  Reduction:      {raw_count:,} → {final_count:,} ({raw_count/final_count:.0f}:1)")

    print(f"\n  === State distribution (top 10) ===")
    geography.groupBy("state").agg(
        F.count("*").alias("zip_count"),
        F.sum("sample_count").alias("total_samples"),
    ).orderBy(F.desc("zip_count")).show(10, truncate=False)

    print(f"  === Sample output ===")
    geography.orderBy("zip_prefix").show(10, truncate=False)

    # ── Shuffle analysis ──
    print(f"  === Spark Execution Plan (canonical city resolution) ===")
    canonical_plan = city_counts.groupBy("geolocation_zip_code_prefix").agg(
        F.max("freq")
    )
    canonical_plan.explain()

    spark.stop()
    print(f"\n  Output: {output_path}")
    print("  Done.")


if __name__ == "__main__":
    main()
