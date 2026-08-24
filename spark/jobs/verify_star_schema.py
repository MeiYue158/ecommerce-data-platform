"""Verify star schema integrity and run sample analytical queries."""
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("verify_star_schema")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

gold = "s3a://ecommerce-data/gold"

# ── Load all tables ──
dim_date = spark.read.parquet(f"{gold}/dim_date")
dim_customer = spark.read.parquet(f"{gold}/dim_customer")
dim_product = spark.read.parquet(f"{gold}/dim_product")
dim_seller = spark.read.parquet(f"{gold}/dim_seller")
dim_geo = spark.read.parquet(f"{gold}/dim_geography")
dim_pay_type = spark.read.parquet(f"{gold}/dim_payment_type")
fact_items = spark.read.parquet(f"{gold}/fact_order_items")
fact_orders = spark.read.parquet(f"{gold}/fact_orders")
fact_payments = spark.read.parquet(f"{gold}/fact_payments")
fact_reviews = spark.read.parquet(f"{gold}/fact_reviews")

print(f"\n{'='*60}")
print(f"  STAR SCHEMA VERIFICATION")
print(f"{'='*60}\n")

# ── 1. Row counts ──
print("  --- Table Sizes ---")
for name, df in [
    ("dim_date", dim_date), ("dim_customer", dim_customer),
    ("dim_product", dim_product), ("dim_seller", dim_seller),
    ("dim_geography", dim_geo), ("dim_payment_type", dim_pay_type),
    ("fact_order_items", fact_items), ("fact_orders", fact_orders),
    ("fact_payments", fact_payments), ("fact_reviews", fact_reviews),
]:
    print(f"  {name:<20} {df.count():>10,} rows  {len(df.columns):>3} cols")

# ── 2. Null SK check (FK resolution quality) ──
print(f"\n  --- Null Surrogate Key Check ---")
checks = [
    ("fact_order_items.customer_sk", fact_items, "customer_sk"),
    ("fact_order_items.product_sk", fact_items, "product_sk"),
    ("fact_order_items.seller_sk", fact_items, "seller_sk"),
    ("fact_order_items.purchase_date_sk", fact_items, "purchase_date_sk"),
    ("fact_orders.customer_sk", fact_orders, "customer_sk"),
    ("fact_payments.customer_sk", fact_payments, "customer_sk"),
    ("fact_payments.payment_type_sk", fact_payments, "payment_type_sk"),
    ("fact_reviews.customer_sk", fact_reviews, "customer_sk"),
]
for label, df, col in checks:
    nulls = df.filter(F.col(col).isNull()).count()
    status = "PASS" if nulls == 0 else f"WARN ({nulls:,} nulls)"
    print(f"  [{status:>6}] {label}")

# ── 3. Sample query: Monthly revenue ──
print(f"\n  --- Monthly Revenue (top 10) ---")
(
    fact_items
    .join(dim_date, fact_items.purchase_date_sk == dim_date.date_sk, "inner")
    .groupBy("year_month")
    .agg(
        F.sum("price").alias("revenue"),
        F.sum("freight_value").alias("freight"),
        F.countDistinct("order_id").alias("orders"),
    )
    .orderBy("year_month")
    .show(20, truncate=False)
)

# ── 4. Sample query: Revenue by category (top 10) ──
print(f"  --- Revenue by Category (top 10) ---")
(
    fact_items
    .join(dim_product, "product_sk", "inner")
    .groupBy("category")
    .agg(
        F.sum("price").alias("revenue"),
        F.count("*").alias("items_sold"),
    )
    .orderBy(F.desc("revenue"))
    .show(10, truncate=False)
)

# ── 5. Sample query: Avg review score by delivery delay ──
print(f"  --- Avg Review Score vs Delivery Delay ---")
(
    fact_orders
    .filter(F.col("delivery_delay_days").isNotNull())
    .withColumn("delay_bucket", F.when(F.col("delivery_delay_days") <= 0, "on_time")
        .when(F.col("delivery_delay_days") <= 7, "1-7 days late")
        .when(F.col("delivery_delay_days") <= 14, "8-14 days late")
        .otherwise("15+ days late"))
    .join(fact_reviews.select("order_id", "review_score"), "order_id", "inner")
    .groupBy("delay_bucket")
    .agg(
        F.round(F.avg("review_score"), 2).alias("avg_score"),
        F.count("*").alias("orders"),
    )
    .orderBy("delay_bucket")
    .show(truncate=False)
)

# ── 6. Grain verification ──
print(f"  --- Grain Verification ---")
items_grain = fact_items.groupBy("order_id", "order_item_id").count().filter(F.col("count") > 1).count()
orders_grain = fact_orders.groupBy("order_id").count().filter(F.col("count") > 1).count()
payments_grain = fact_payments.groupBy("order_id", "payment_sequential").count().filter(F.col("count") > 1).count()
reviews_grain = fact_reviews.groupBy("review_id").count().filter(F.col("count") > 1).count()
print(f"  fact_order_items grain violations: {items_grain}")
print(f"  fact_orders grain violations:      {orders_grain}")
print(f"  fact_payments grain violations:    {payments_grain}")
print(f"  fact_reviews grain violations:     {reviews_grain}")

spark.stop()
