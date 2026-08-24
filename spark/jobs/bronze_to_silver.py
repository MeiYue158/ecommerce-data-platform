"""
Bronze → Silver Transformation

Cleans, standardizes, deduplicates, and validates Bronze data.
Valid records go to silver/, rejected records go to silver_rejected/.

Usage:
    spark-submit bronze_to_silver.py <ingestion_date> <table_name>
    spark-submit bronze_to_silver.py 2026-08-24 orders
    spark-submit bronze_to_silver.py 2026-08-24 all
"""
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


BRONZE_BASE = "s3a://ecommerce-data/bronze"
SILVER_BASE = "s3a://ecommerce-data/silver"
SILVER_REJECTED_BASE = "s3a://ecommerce-data/silver_rejected"

VALID_ORDER_STATUSES = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
]

VALID_PAYMENT_TYPES = [
    "credit_card", "boleto", "voucher", "debit_card", "not_defined",
]

VALID_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
    "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
    "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]


def create_spark_session(app_name="bronze_to_silver"):
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


def read_bronze(spark, table, ingestion_date):
    """Read Bronze table, drop ingestion metadata columns."""
    path = f"{BRONZE_BASE}/{table}/ingestion_date={ingestion_date}"
    df = spark.read.parquet(path)
    data_cols = [c for c in df.columns if not c.startswith("_")]
    return df.select(data_cols)


def write_silver(df_valid, df_rejected, table, ingestion_date):
    """Write valid and rejected DataFrames to Silver paths."""
    valid_path = f"{SILVER_BASE}/{table}/ingestion_date={ingestion_date}"
    rejected_path = f"{SILVER_REJECTED_BASE}/{table}/ingestion_date={ingestion_date}"

    df_valid.write.mode("overwrite").parquet(valid_path)

    if df_rejected.count() > 0:
        df_rejected.write.mode("overwrite").parquet(rejected_path)

    valid_count = df_valid.count()
    rejected_count = df_rejected.count()
    print(f"[{table}] valid={valid_count:,}  rejected={rejected_count:,}")
    return valid_count, rejected_count


# ──────────────────────────────────────────────────────────────
# Table-specific transformations
# ──────────────────────────────────────────────────────────────

def transform_orders(spark, ingestion_date):
    df = read_bronze(spark, "orders", ingestion_date)

    # Schema standardization: ensure timestamp types
    for col_name in [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        df = df.withColumn(col_name, F.col(col_name).cast("timestamp"))

    # Standardize order_status to lowercase
    df = df.withColumn("order_status", F.lower(F.trim(F.col("order_status"))))

    # Validation
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("order_id").isNull(), "null_order_id")
        .when(~F.col("order_status").isin(VALID_ORDER_STATUSES), "invalid_status")
        .when(F.col("order_purchase_timestamp").isNull(), "null_purchase_timestamp")
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    # Deduplicate by order_id (keep first occurrence)
    valid = valid.dropDuplicates(["order_id"])

    return write_silver(valid, rejected, "orders", ingestion_date)


def transform_order_items(spark, ingestion_date):
    df = read_bronze(spark, "order_items", ingestion_date)

    # Schema standardization
    df = (
        df
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .withColumn("shipping_limit_date", F.col("shipping_limit_date").cast("timestamp"))
    )

    # Validation
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("order_id").isNull(), "null_order_id")
        .when(F.col("product_id").isNull(), "null_product_id")
        .when(F.col("seller_id").isNull(), "null_seller_id")
        .when(F.col("price") < 0, "negative_price")
        .when(F.col("freight_value") < 0, "negative_freight")
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    # Deduplicate by composite key
    valid = valid.dropDuplicates(["order_id", "order_item_id"])

    return write_silver(valid, rejected, "order_items", ingestion_date)


def transform_order_payments(spark, ingestion_date):
    df = read_bronze(spark, "order_payments", ingestion_date)

    # Schema standardization
    df = (
        df
        .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
        .withColumn("payment_installments", F.col("payment_installments").cast("int"))
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    )

    # Validation
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("order_id").isNull(), "null_order_id")
        .when(~F.col("payment_type").isin(VALID_PAYMENT_TYPES), "invalid_payment_type")
        .when(F.col("payment_value") < 0, "negative_payment_value")
        .when(F.col("payment_installments") < 0, "negative_installments")
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    valid = valid.dropDuplicates(["order_id", "payment_sequential"])

    return write_silver(valid, rejected, "order_payments", ingestion_date)


def transform_order_reviews(spark, ingestion_date):
    df = read_bronze(spark, "order_reviews", ingestion_date)

    # Schema standardization
    df = (
        df
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("review_creation_date", F.col("review_creation_date").cast("timestamp"))
        .withColumn("review_answer_timestamp", F.col("review_answer_timestamp").cast("timestamp"))
    )

    # Validation
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("review_id").isNull(), "null_review_id")
        .when(F.col("order_id").isNull(), "null_order_id")
        .when(F.col("review_score").isNull(), "null_review_score")
        .when(
            ~F.col("review_score").between(1, 5), "invalid_review_score"
        )
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    # Deduplicate by review_id — keep the latest review_answer_timestamp
    window = Window.partitionBy("review_id").orderBy(
        F.col("review_answer_timestamp").desc_nulls_last()
    )
    valid = (
        valid
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    return write_silver(valid, rejected, "order_reviews", ingestion_date)


def transform_customers(spark, ingestion_date):
    df = read_bronze(spark, "customers", ingestion_date)

    # Normalize city names to title case
    df = df.withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))

    # Standardize state to uppercase
    df = df.withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))

    # Cast zip prefix to string (consistent type)
    df = df.withColumn(
        "customer_zip_code_prefix",
        F.lpad(F.col("customer_zip_code_prefix").cast("string"), 5, "0"),
    )

    # Validation
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("customer_id").isNull(), "null_customer_id")
        .when(F.col("customer_unique_id").isNull(), "null_customer_unique_id")
        .when(~F.col("customer_state").isin(VALID_STATES), "invalid_state")
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    valid = valid.dropDuplicates(["customer_id"])

    return write_silver(valid, rejected, "customers", ingestion_date)


def transform_products(spark, ingestion_date):
    df = read_bronze(spark, "products", ingestion_date)

    # Join with category translation for English names
    cat_path = f"{BRONZE_BASE}/category_translation/ingestion_date={ingestion_date}"
    cat_df = spark.read.parquet(cat_path)
    cat_cols = [c for c in cat_df.columns if not c.startswith("_")]
    cat_df = cat_df.select(cat_cols)

    df = df.join(
        cat_df,
        on="product_category_name",
        how="left",
    )

    # Use English name as primary, fallback to Portuguese
    df = df.withColumn(
        "product_category",
        F.coalesce(
            F.col("product_category_name_english"),
            F.col("product_category_name"),
        ),
    )

    # Cast numeric columns
    for col_name in [
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ]:
        df = df.withColumn(col_name, F.col(col_name).cast("int"))

    # Validation
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("product_id").isNull(), "null_product_id")
        .when(F.col("product_weight_g") < 0, "negative_weight")
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    # Drop intermediate columns, keep clean schema
    valid = valid.drop("product_category_name_english")

    valid = valid.dropDuplicates(["product_id"])

    return write_silver(valid, rejected, "products", ingestion_date)


def transform_sellers(spark, ingestion_date):
    df = read_bronze(spark, "sellers", ingestion_date)

    # Normalize city names and state codes
    df = (
        df
        .withColumn("seller_city", F.initcap(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn(
            "seller_zip_code_prefix",
            F.lpad(F.col("seller_zip_code_prefix").cast("string"), 5, "0"),
        )
    )

    # Validation
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("seller_id").isNull(), "null_seller_id")
        .when(~F.col("seller_state").isin(VALID_STATES), "invalid_state")
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    valid = valid.dropDuplicates(["seller_id"])

    return write_silver(valid, rejected, "sellers", ingestion_date)


def transform_category_translation(spark, ingestion_date):
    df = read_bronze(spark, "category_translation", ingestion_date)

    # Minimal transformation — reference table
    df = df.withColumn(
        "_rejection_reason",
        F.when(F.col("product_category_name").isNull(), "null_category_name")
    )

    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = df.filter(F.col("_rejection_reason").isNotNull())

    valid = valid.dropDuplicates(["product_category_name"])

    return write_silver(valid, rejected, "category_translation", ingestion_date)


# ──────────────────────────────────────────────────────────────
# Referential integrity check (run after all tables are in Silver)
# ──────────────────────────────────────────────────────────────

def check_referential_integrity(spark, ingestion_date):
    """Check FK relationships across Silver tables. Reports only, does not reject."""
    print(f"\n{'='*60}")
    print(f"  REFERENTIAL INTEGRITY CHECK")
    print(f"{'='*60}\n")

    def read_silver(table):
        return spark.read.parquet(
            f"{SILVER_BASE}/{table}/ingestion_date={ingestion_date}"
        )

    orders = read_silver("orders")
    order_items = read_silver("order_items")
    order_payments = read_silver("order_payments")
    order_reviews = read_silver("order_reviews")
    customers = read_silver("customers")
    products = read_silver("products")
    sellers = read_silver("sellers")

    checks = [
        ("order_items.order_id → orders", order_items, "order_id", orders, "order_id"),
        ("order_items.product_id → products", order_items, "product_id", products, "product_id"),
        ("order_items.seller_id → sellers", order_items, "seller_id", sellers, "seller_id"),
        ("orders.customer_id → customers", orders, "customer_id", customers, "customer_id"),
        ("order_payments.order_id → orders", order_payments, "order_id", orders, "order_id"),
        ("order_reviews.order_id → orders", order_reviews, "order_id", orders, "order_id"),
    ]

    all_pass = True
    for label, child_df, child_col, parent_df, parent_col in checks:
        orphans = child_df.join(
            parent_df.select(parent_col).distinct(),
            child_df[child_col] == parent_df[parent_col],
            "left_anti",
        ).count()

        status = "PASS" if orphans == 0 else "WARN"
        if orphans > 0:
            all_pass = False
        print(f"  [{status}] {label}: {orphans:,} orphan(s)")

    return all_pass


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

TRANSFORMS = {
    "orders": transform_orders,
    "order_items": transform_order_items,
    "order_payments": transform_order_payments,
    "order_reviews": transform_order_reviews,
    "customers": transform_customers,
    "products": transform_products,
    "sellers": transform_sellers,
    "category_translation": transform_category_translation,
}


def main():
    if len(sys.argv) < 3:
        print("Usage: bronze_to_silver.py <ingestion_date> <table_name|all|check_refs>")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    target = sys.argv[2]

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    if target == "check_refs":
        ok = check_referential_integrity(spark, ingestion_date)
        spark.stop()
        sys.exit(0 if ok else 0)  # Warnings don't fail the pipeline

    elif target == "all":
        print(f"\n{'='*60}")
        print(f"  BRONZE → SILVER — {ingestion_date}")
        print(f"{'='*60}\n")
        total_valid = 0
        total_rejected = 0
        for name, fn in TRANSFORMS.items():
            v, r = fn(spark, ingestion_date)
            total_valid += v
            total_rejected += r
        print(f"\nTotal: {total_valid:,} valid, {total_rejected:,} rejected")

    else:
        if target not in TRANSFORMS:
            print(f"Unknown table: {target}")
            print(f"Available: {list(TRANSFORMS.keys())}")
            sys.exit(1)
        TRANSFORMS[target](spark, ingestion_date)

    spark.stop()


if __name__ == "__main__":
    main()
