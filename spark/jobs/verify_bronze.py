"""Quick verification of Bronze layer data in MinIO."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("verify_bronze")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

tables = [
    "orders", "order_items", "order_payments", "order_reviews",
    "customers", "products", "sellers", "geolocation", "category_translation",
]

print("\n=== Bronze Layer Verification ===\n")
for t in tables:
    path = f"s3a://ecommerce-data/bronze/{t}/ingestion_date=2026-08-24"
    df = spark.read.parquet(path)
    cols = [c for c in df.columns if not c.startswith("_")]
    meta = [c for c in df.columns if c.startswith("_")]
    print(f"{t:25s}  rows={df.count():>10,}  cols={len(cols)}  meta={meta}")

print("\n=== Sample: orders (first 3 rows) ===\n")
orders = spark.read.parquet("s3a://ecommerce-data/bronze/orders/ingestion_date=2026-08-24")
orders.printSchema()
orders.show(3, truncate=30)

spark.stop()
