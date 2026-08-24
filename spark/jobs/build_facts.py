"""
Build Fact Tables

Creates star-schema fact tables by joining Silver data with Gold dimensions
to resolve surrogate keys.

Fact Tables:
    fact_order_items  — grain: one row per order item
    fact_orders       — grain: one row per order
    fact_payments     — grain: one row per payment transaction
    fact_reviews      — grain: one row per review

Usage:
    spark-submit build_facts.py <ingestion_date> [fact_name|all]
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_BASE = "s3a://ecommerce-data/silver"
GOLD_BASE = "s3a://ecommerce-data/gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("build_facts")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_silver(spark, table, ingestion_date, fallback_date="2026-08-24"):
    path = f"{SILVER_BASE}/{table}/ingestion_date={ingestion_date}"
    try:
        return spark.read.parquet(path)
    except Exception:
        return spark.read.parquet(f"{SILVER_BASE}/{table}/ingestion_date={fallback_date}")


def read_dim(spark, name):
    return spark.read.parquet(f"{GOLD_BASE}/{name}")


def write_gold(df, name):
    path = f"{GOLD_BASE}/{name}"
    df.write.mode("overwrite").parquet(path)
    count = df.count()
    print(f"  [{name}] {count:,} rows -> {path}")
    return count


def to_date_sk(col):
    """Convert timestamp column to date_sk (YYYYMMDD integer)."""
    return F.date_format(col, "yyyyMMdd").cast("int")


# ──────────────────────────────────────────────────────────────
# fact_order_items
#   Grain: one row per order item (order_id × order_item_id)
#   Measures: price, freight_value (additive)
# ──────────────────────────────────────────────────────────────

def build_fact_order_items(spark, ingestion_date):
    items = read_silver(spark, "order_items", ingestion_date)
    orders = read_silver(spark, "orders", ingestion_date)
    dim_customer = read_dim(spark, "dim_customer")
    dim_product = read_dim(spark, "dim_product")
    dim_seller = read_dim(spark, "dim_seller")

    # Join order_items with orders to get customer_id and timestamps
    df = items.join(
        orders.select(
            "order_id", "customer_id",
            "order_purchase_timestamp", "order_delivered_customer_date",
            "order_estimated_delivery_date", "order_status",
        ),
        on="order_id",
        how="inner",
    )

    # Resolve surrogate keys via dimension lookups
    df = (
        df
        .join(
            dim_customer.select("customer_sk", "customer_id"),
            on="customer_id", how="left",
        )
        .join(
            dim_product.select("product_sk", "product_id"),
            on="product_id", how="left",
        )
        .join(
            dim_seller.select("seller_sk", "seller_id"),
            on="seller_id", how="left",
        )
    )

    # Build date surrogate keys
    df = (
        df
        .withColumn("purchase_date_sk", to_date_sk("order_purchase_timestamp"))
        .withColumn("delivery_date_sk", to_date_sk("order_delivered_customer_date"))
        .withColumn("shipping_limit_date_sk", to_date_sk("shipping_limit_date"))
    )

    # Select final fact columns
    fact = df.select(
        # Degenerate dimensions
        "order_id",
        "order_item_id",
        # Surrogate keys
        "customer_sk",
        "product_sk",
        "seller_sk",
        "purchase_date_sk",
        "delivery_date_sk",
        "shipping_limit_date_sk",
        # Measures (additive)
        F.col("price").cast("double"),
        F.col("freight_value").cast("double"),
        # Status for filtering
        "order_status",
    )

    return write_gold(fact, "fact_order_items")


# ──────────────────────────────────────────────────────────────
# fact_orders
#   Grain: one row per order
#   Measures: order_value, item_count, freight_total,
#             delivery_days, delivery_delay_days
# ──────────────────────────────────────────────────────────────

def build_fact_orders(spark, ingestion_date):
    orders = read_silver(spark, "orders", ingestion_date)
    items = read_silver(spark, "order_items", ingestion_date)
    dim_customer = read_dim(spark, "dim_customer")

    # Aggregate order-level metrics from items
    order_metrics = items.groupBy("order_id").agg(
        F.sum("price").alias("order_value"),
        F.sum("freight_value").alias("freight_total"),
        F.count("*").alias("item_count"),
    )

    df = orders.join(order_metrics, on="order_id", how="left")

    # Resolve customer SK
    df = df.join(
        dim_customer.select("customer_sk", "customer_id"),
        on="customer_id", how="left",
    )

    # Compute delivery metrics
    df = (
        df
        .withColumn("purchase_date_sk", to_date_sk("order_purchase_timestamp"))
        .withColumn("approved_date_sk", to_date_sk("order_approved_at"))
        .withColumn("delivered_carrier_date_sk", to_date_sk("order_delivered_carrier_date"))
        .withColumn("delivery_date_sk", to_date_sk("order_delivered_customer_date"))
        .withColumn("estimated_delivery_date_sk", to_date_sk("order_estimated_delivery_date"))
        .withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
        )
        .withColumn(
            "estimated_delivery_days",
            F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"),
        )
        .withColumn(
            "delivery_delay_days",
            F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"),
        )
    )

    fact = df.select(
        # Degenerate dimension
        "order_id",
        # Surrogate keys
        "customer_sk",
        "purchase_date_sk",
        "approved_date_sk",
        "delivered_carrier_date_sk",
        "delivery_date_sk",
        "estimated_delivery_date_sk",
        # Status
        "order_status",
        # Measures (additive)
        F.coalesce("order_value", F.lit(0.0)).alias("order_value"),
        F.coalesce("freight_total", F.lit(0.0)).alias("freight_total"),
        F.coalesce("item_count", F.lit(0)).alias("item_count"),
        # Delivery measures (semi-additive — meaningful per order, not summed)
        "delivery_days",
        "estimated_delivery_days",
        "delivery_delay_days",
    )

    return write_gold(fact, "fact_orders")


# ──────────────────────────────────────────────────────────────
# fact_payments
#   Grain: one row per payment transaction (order_id × payment_sequential)
#   Measures: payment_value (additive), installments
# ──────────────────────────────────────────────────────────────

def build_fact_payments(spark, ingestion_date):
    payments = read_silver(spark, "order_payments", ingestion_date)
    orders = read_silver(spark, "orders", ingestion_date)
    dim_customer = read_dim(spark, "dim_customer")
    dim_payment_type = read_dim(spark, "dim_payment_type")

    # Join with orders for customer_id and purchase date
    df = payments.join(
        orders.select("order_id", "customer_id", "order_purchase_timestamp"),
        on="order_id", how="inner",
    )

    # Resolve surrogate keys
    df = (
        df
        .join(
            dim_customer.select("customer_sk", "customer_id"),
            on="customer_id", how="left",
        )
        .join(
            dim_payment_type.select("payment_type_sk", "payment_type"),
            on="payment_type", how="left",
        )
        .withColumn("payment_date_sk", to_date_sk("order_purchase_timestamp"))
    )

    fact = df.select(
        # Degenerate dimensions
        "order_id",
        "payment_sequential",
        # Surrogate keys
        "customer_sk",
        "payment_type_sk",
        "payment_date_sk",
        # Measures
        F.col("payment_installments").alias("installments"),
        F.col("payment_value").cast("double"),
    )

    return write_gold(fact, "fact_payments")


# ──────────────────────────────────────────────────────────────
# fact_reviews
#   Grain: one row per review
#   Measures: review_score (semi-additive — avg is meaningful, sum is not)
# ──────────────────────────────────────────────────────────────

def build_fact_reviews(spark, ingestion_date):
    reviews = read_silver(spark, "order_reviews", ingestion_date)
    orders = read_silver(spark, "orders", ingestion_date)
    dim_customer = read_dim(spark, "dim_customer")

    # Join with orders for customer_id
    df = reviews.join(
        orders.select("order_id", "customer_id"),
        on="order_id", how="inner",
    )

    # Resolve surrogate keys
    df = (
        df
        .join(
            dim_customer.select("customer_sk", "customer_id"),
            on="customer_id", how="left",
        )
        .withColumn("review_created_date_sk", to_date_sk("review_creation_date"))
        .withColumn("review_answer_date_sk", to_date_sk("review_answer_timestamp"))
    )

    fact = df.select(
        # Degenerate dimensions
        "review_id",
        "order_id",
        # Surrogate keys
        "customer_sk",
        "review_created_date_sk",
        "review_answer_date_sk",
        # Measures
        "review_score",
        # Attributes (for text analysis if needed)
        "review_comment_title",
        "review_comment_message",
    )

    return write_gold(fact, "fact_reviews")


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

FACTS = {
    "fact_order_items": build_fact_order_items,
    "fact_orders": build_fact_orders,
    "fact_payments": build_fact_payments,
    "fact_reviews": build_fact_reviews,
}


def main():
    if len(sys.argv) < 2:
        print("Usage: build_facts.py <ingestion_date> [fact_name|all]")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else "all"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  BUILD FACTS — {ingestion_date}")
    print(f"{'='*60}\n")

    if target == "all":
        for name, fn in FACTS.items():
            fn(spark, ingestion_date)
    else:
        if target not in FACTS:
            print(f"Unknown fact: {target}")
            print(f"Available: {list(FACTS.keys())}")
            sys.exit(1)
        FACTS[target](spark, ingestion_date)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
