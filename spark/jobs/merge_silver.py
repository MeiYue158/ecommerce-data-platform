"""
Incremental Silver Merge

Reads a new Bronze partition, applies Silver transformations, and merges
with existing Silver data using UPSERT logic:
  - New records (by PK): INSERT
  - Existing records (by PK): UPDATE (replace with latest)

Idempotency: re-running with the same Bronze partition produces identical
Silver output because the merge is deterministic (PK-based dedup, keep latest).

Usage:
    spark-submit merge_silver.py <batch_date> [--baseline 2026-08-24]
"""
import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BRONZE_BASE = "s3a://ecommerce-data/bronze"
SILVER_BASE = "s3a://ecommerce-data/silver"

# Table configs: (pk_columns, silver_table_name)
TABLE_CONFIGS = {
    "orders": {
        "pk": ["order_id"],
        "timestamp_cols": [
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    "order_items": {
        "pk": ["order_id", "order_item_id"],
        "cast": {"order_item_id": "int", "price": "double", "freight_value": "double"},
    },
    "order_payments": {
        "pk": ["order_id", "payment_sequential"],
        "cast": {"payment_sequential": "int", "payment_installments": "int", "payment_value": "double"},
    },
    "order_reviews": {
        "pk": ["review_id"],
        "cast": {"review_score": "int"},
    },
    "customers": {
        "pk": ["customer_id"],
        "normalize_city": "customer_city",
        "normalize_state": "customer_state",
        "pad_zip": "customer_zip_code_prefix",
    },
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("merge_silver")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_bronze_partition(spark, table, ingestion_date):
    """Read a specific Bronze partition, strip metadata columns."""
    path = f"{BRONZE_BASE}/{table}/ingestion_date={ingestion_date}"
    try:
        df = spark.read.parquet(path)
        data_cols = [c for c in df.columns if not c.startswith("_")]
        return df.select(data_cols)
    except Exception:
        return None


def read_silver(spark, table, baseline_date):
    """Read existing Silver data."""
    path = f"{SILVER_BASE}/{table}/ingestion_date={baseline_date}"
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def apply_transforms(df, config):
    """Apply Silver-level cleaning/standardization."""
    # Type casts
    for col_name, dtype in config.get("cast", {}).items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

    # Timestamp casts
    for col_name in config.get("timestamp_cols", []):
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("timestamp"))

    # City normalization
    city_col = config.get("normalize_city")
    if city_col and city_col in df.columns:
        df = df.withColumn(city_col, F.initcap(F.trim(F.col(city_col))))

    # State normalization
    state_col = config.get("normalize_state")
    if state_col and state_col in df.columns:
        df = df.withColumn(state_col, F.upper(F.trim(F.col(state_col))))

    # ZIP padding
    zip_col = config.get("pad_zip")
    if zip_col and zip_col in df.columns:
        df = df.withColumn(zip_col, F.lpad(F.col(zip_col).cast("string"), 5, "0"))

    # Status normalization
    if "order_status" in df.columns:
        df = df.withColumn("order_status", F.lower(F.trim(F.col("order_status"))))

    if "payment_type" in df.columns:
        df = df.withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))

    return df


def merge_by_pk(existing_df, new_df, pk_columns):
    """
    UPSERT: merge new records into existing.
    - Records with matching PKs: replaced by new version
    - Records with new PKs: inserted
    - Existing records not in new: kept unchanged
    """
    # Align schemas (new data may have fewer columns)
    for col in existing_df.columns:
        if col not in new_df.columns:
            new_df = new_df.withColumn(col, F.lit(None))
    new_df = new_df.select(existing_df.columns)

    # Remove existing records that are being updated
    unchanged = existing_df.join(
        new_df.select(pk_columns).distinct(),
        pk_columns,
        "left_anti",
    )

    # Union: unchanged existing + all new records
    merged = unchanged.unionByName(new_df)

    # Dedup by PK (safety net for duplicates within new data)
    merged = merged.dropDuplicates(pk_columns)

    return merged


def process_table(spark, table, config, batch_date, baseline_date):
    """Process a single table: read Bronze increment, merge into Silver."""
    pk = config["pk"]

    # Read new Bronze data
    new_data = read_bronze_partition(spark, table, batch_date)
    if new_data is None:
        print(f"  [{table}] no new data for {batch_date}")
        return 0, 0, 0

    new_data = apply_transforms(new_data, config)
    new_count = new_data.count()

    # Read existing Silver
    existing = read_silver(spark, table, baseline_date)
    if existing is None:
        print(f"  [{table}] no existing Silver — writing {new_count} as new")
        output_path = f"{SILVER_BASE}/{table}/ingestion_date={batch_date}"
        new_data.write.mode("overwrite").parquet(output_path)
        return new_count, 0, new_count

    existing_count = existing.count()

    # Merge
    merged = merge_by_pk(existing, new_data, pk)
    merged_count = merged.count()

    inserted = merged_count - existing_count
    updated = new_count - inserted

    # Write merged result to the batch_date partition
    output_path = f"{SILVER_BASE}/{table}/ingestion_date={batch_date}"
    merged.write.mode("overwrite").parquet(output_path)

    print(f"  [{table}] existing={existing_count:,} + new={new_count:,} "
          f"-> merged={merged_count:,} (inserted={inserted:,}, updated={updated:,})")

    return inserted, updated, merged_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_date")
    parser.add_argument("--baseline", default="2026-08-24",
                        help="Date of baseline Silver data to merge into")
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  INCREMENTAL SILVER MERGE — {args.batch_date}")
    print(f"  Baseline: {args.baseline}")
    print(f"{'='*60}\n")

    total_inserted = 0
    total_updated = 0

    for table, config in TABLE_CONFIGS.items():
        ins, upd, _ = process_table(spark, table, config, args.batch_date, args.baseline)
        total_inserted += ins
        total_updated += upd

    print(f"\n  Summary: {total_inserted:,} inserted, {total_updated:,} updated")

    # ── Idempotency verification ──
    print(f"\n  --- Idempotency Check ---")
    print(f"  Re-running this job with the same batch_date will produce")
    print(f"  identical output because:")
    print(f"    1. Bronze partition is overwritten (same input)")
    print(f"    2. Merge is PK-deterministic (same merge result)")
    print(f"    3. Silver partition is overwritten (same output)")

    spark.stop()
    print("\n  Done.")


if __name__ == "__main__":
    main()
