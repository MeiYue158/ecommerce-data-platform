"""
Bronze Ingestion Job

Reads source CSV files and writes them to the Bronze layer in MinIO as Parquet,
adding ingestion metadata columns for auditability and replay.

Usage:
    spark-submit ingest_to_bronze.py <ingestion_date> [table_name]

Examples:
    spark-submit ingest_to_bronze.py 2026-08-24           # all tables
    spark-submit ingest_to_bronze.py 2026-08-24 orders    # single table
"""
import sys
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SOURCE_TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

BRONZE_PATH = "s3a://ecommerce-data/bronze"
SOURCE_DIR = "/opt/data/seed"


def create_spark_session(app_name="bronze_ingestion"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ingest_table(spark, table_name, file_name, ingestion_date, batch_id):
    """Read a source CSV, add metadata columns, write as Parquet to Bronze."""
    source_path = f"{SOURCE_DIR}/{file_name}"
    target_path = f"{BRONZE_PATH}/{table_name}/ingestion_date={ingestion_date}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(source_path)
    )

    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_system", F.lit("olist"))
    )

    # Overwrite the partition for idempotent reruns
    df.write.mode("overwrite").parquet(target_path)

    count = df.count()
    print(f"[{table_name}] {count} rows -> {target_path}")
    return count


def main():
    if len(sys.argv) < 2:
        print("Usage: ingest_to_bronze.py <ingestion_date> [table_name]")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else None
    batch_id = f"batch_{ingestion_date}_{uuid.uuid4().hex[:8]}"

    print(f"=== Bronze Ingestion ===")
    print(f"Date:     {ingestion_date}")
    print(f"Batch ID: {batch_id}")
    print(f"Tables:   {table_name or 'ALL'}")
    print()

    spark = create_spark_session()

    try:
        if table_name:
            if table_name not in SOURCE_TABLES:
                print(f"Unknown table: {table_name}")
                print(f"Available: {list(SOURCE_TABLES.keys())}")
                sys.exit(1)
            ingest_table(spark, table_name, SOURCE_TABLES[table_name],
                         ingestion_date, batch_id)
        else:
            total = 0
            for name, file in SOURCE_TABLES.items():
                count = ingest_table(spark, name, file, ingestion_date, batch_id)
                total += count
            print(f"\nTotal: {total} rows across {len(SOURCE_TABLES)} tables")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
