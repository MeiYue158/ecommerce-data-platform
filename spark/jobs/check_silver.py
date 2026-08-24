"""Quick Silver layer quality check."""
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("silver_check")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

base = "s3a://ecommerce-data/silver"
date = "2026-08-24"

print(f"\n{'='*60}")
print(f"  SILVER LAYER SUMMARY")
print(f"{'='*60}\n")

tables = [
    "orders", "order_items", "order_payments", "order_reviews",
    "customers", "products", "sellers", "category_translation",
]
for t in tables:
    df = spark.read.parquet(f"{base}/{t}/ingestion_date={date}")
    print(f"  {t:<25} {df.count():>10,} rows  {len(df.columns)} cols")

print(f"\n  === Customers: normalized cities ===")
c = spark.read.parquet(f"{base}/customers/ingestion_date={date}")
c.select("customer_city", "customer_state", "customer_zip_code_prefix").show(5, truncate=False)

print(f"  === Products: English categories ===")
p = spark.read.parquet(f"{base}/products/ingestion_date={date}")
p.select("product_id", "product_category_name", "product_category").show(5, truncate=False)

print(f"  === Reviews: dedup check ===")
r = spark.read.parquet(f"{base}/order_reviews/ingestion_date={date}")
dup = r.groupBy("review_id").count().filter(F.col("count") > 1).count()
print(f"  Remaining duplicate review_ids: {dup}")

spark.stop()
