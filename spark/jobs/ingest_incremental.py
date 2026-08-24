"""
Incremental Bronze Ingestion

Reads generated incremental CSV files and loads them into Bronze.
Combines new + late-arriving records into a single Bronze partition per table.

Idempotency: overwrite mode on the ingestion_date partition.
Re-running the same batch date produces identical results.

Usage:
    spark-submit ingest_incremental.py <batch_date>
"""
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INCREMENTAL_DIR = "/opt/data/incremental"
BRONZE_BASE = "s3a://ecommerce-data/bronze"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ingest_incremental")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_csv_if_exists(spark, path):
    """Read CSV, return None if path doesn't exist."""
    try:
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    except Exception:
        return None


def ingest_table(spark, batch_date, batch_id, table_name, csv_paths):
    """Read one or more CSV sources, add metadata, write to Bronze partition."""
    dfs = []
    for path in csv_paths:
        df = read_csv_if_exists(spark, f"{INCREMENTAL_DIR}/{batch_date}/{path}")
        if df is not None and df.count() > 0:
            dfs.append(df)

    if not dfs:
        print(f"  [{table_name}] no data")
        return 0

    # Union all sources (e.g., new_orders + late_orders)
    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    # Add ingestion metadata
    combined = (
        combined
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(",".join(csv_paths)))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_system", F.lit("incremental_generator"))
    )

    # Write to Bronze partition (overwrite for idempotency)
    target = f"{BRONZE_BASE}/{table_name}/ingestion_date={batch_date}"
    combined.write.mode("overwrite").parquet(target)

    count = combined.count()
    print(f"  [{table_name}] {count:,} rows -> {target}")
    return count


def main():
    if len(sys.argv) < 2:
        print("Usage: ingest_incremental.py <batch_date>")
        sys.exit(1)

    batch_date = sys.argv[1]
    batch_id = f"inc_{batch_date}_{uuid.uuid4().hex[:8]}"
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  INCREMENTAL BRONZE INGESTION — {batch_date}")
    print(f"  Batch ID: {batch_id}")
    print(f"{'='*60}\n")

    # Each table may have new + late-arriving sources
    tables = {
        "orders": ["new_orders", "late_orders"],
        "order_items": ["new_order_items", "late_order_items"],
        "order_payments": ["new_order_payments", "late_order_payments"],
        "order_reviews": ["new_order_reviews"],
        "customers": ["customer_updates"],
    }

    total = 0
    for table_name, csv_paths in tables.items():
        count = ingest_table(spark, batch_date, batch_id, table_name, csv_paths)
        total += count

    print(f"\n  Total: {total:,} rows ingested")
    spark.stop()
    print("  Done.")


if __name__ == "__main__":
    main()
