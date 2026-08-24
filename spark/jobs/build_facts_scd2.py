"""
Rebuild fact_orders with SCD Type 2 customer dimension lookup.

Instead of joining on customer_id alone (Type 1), this performs a
temporal join: the fact gets the customer_sk that was valid at the
time of the order's purchase date.

Usage:
    spark-submit build_facts_scd2.py <ingestion_date>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_BASE = "s3a://ecommerce-data/silver"
GOLD_BASE = "s3a://ecommerce-data/gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("build_facts_scd2")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def to_date_sk(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def main():
    if len(sys.argv) < 2:
        print("Usage: build_facts_scd2.py <ingestion_date>")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  REBUILD FACTS WITH SCD2 LOOKUP — {ingestion_date}")
    print(f"{'='*60}\n")

    # ── Load data ──
    orders = spark.read.parquet(f"{SILVER_BASE}/orders/ingestion_date={ingestion_date}")
    order_items = spark.read.parquet(f"{SILVER_BASE}/order_items/ingestion_date={ingestion_date}")
    dim_customer_scd2 = spark.read.parquet(f"{GOLD_BASE}/dim_customer_scd2")
    dim_product = spark.read.parquet(f"{GOLD_BASE}/dim_product")
    dim_seller = spark.read.parquet(f"{GOLD_BASE}/dim_seller")

    # ── Temporal join: order date falls within customer version range ──
    # This is the core SCD Type 2 lookup
    orders_with_customer = (
        orders
        .join(
            dim_customer_scd2.select(
                "customer_sk", "customer_id", "customer_segment",
                "effective_from", "effective_to",
            ),
            on=(
                (orders["customer_id"] == dim_customer_scd2["customer_id"])
                & (orders["order_purchase_timestamp"].cast("date") >= dim_customer_scd2["effective_from"])
                & (orders["order_purchase_timestamp"].cast("date") <= dim_customer_scd2["effective_to"])
            ),
            how="left",
        )
        .drop(dim_customer_scd2["customer_id"])
    )

    # ── Rebuild fact_orders with SCD2 customer_sk ──
    order_spend = (
        order_items.groupBy("order_id").agg(
            F.sum("price").alias("order_value"),
            F.sum("freight_value").alias("freight_total"),
            F.count("*").alias("item_count"),
        )
    )

    fact_orders = (
        orders_with_customer
        .join(order_spend, "order_id", "left")
        .withColumn("purchase_date_sk", to_date_sk("order_purchase_timestamp"))
        .withColumn("approved_date_sk", to_date_sk("order_approved_at"))
        .withColumn("delivered_carrier_date_sk", to_date_sk("order_delivered_carrier_date"))
        .withColumn("delivery_date_sk", to_date_sk("order_delivered_customer_date"))
        .withColumn("estimated_delivery_date_sk", to_date_sk("order_estimated_delivery_date"))
        .withColumn("delivery_days", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .withColumn("estimated_delivery_days", F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
        .withColumn("delivery_delay_days", F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
        .select(
            "order_id", "customer_sk", "customer_segment",
            "purchase_date_sk", "approved_date_sk",
            "delivered_carrier_date_sk", "delivery_date_sk",
            "estimated_delivery_date_sk", "order_status",
            F.coalesce("order_value", F.lit(0.0)).alias("order_value"),
            F.coalesce("freight_total", F.lit(0.0)).alias("freight_total"),
            F.coalesce("item_count", F.lit(0)).alias("item_count"),
            "delivery_days", "estimated_delivery_days", "delivery_delay_days",
        )
    )

    path = f"{GOLD_BASE}/fact_orders_scd2"
    fact_orders.write.mode("overwrite").parquet(path)
    orders_count = fact_orders.count()
    print(f"  [fact_orders_scd2] {orders_count:,} rows -> {path}")

    # ── Rebuild fact_order_items with SCD2 customer_sk ──
    items_with_orders = (
        order_items
        .join(
            orders_with_customer.select(
                "order_id", "customer_sk", "customer_segment",
                "order_purchase_timestamp", "order_delivered_customer_date",
                "order_status",
            ),
            "order_id", "inner",
        )
        .join(dim_product.select("product_sk", "product_id"), "product_id", "left")
        .join(dim_seller.select("seller_sk", "seller_id"), "seller_id", "left")
    )

    fact_items = items_with_orders.select(
        "order_id", "order_item_id",
        "customer_sk", "customer_segment",
        "product_sk", "seller_sk",
        to_date_sk("order_purchase_timestamp").alias("purchase_date_sk"),
        to_date_sk("order_delivered_customer_date").alias("delivery_date_sk"),
        to_date_sk("shipping_limit_date").alias("shipping_limit_date_sk"),
        F.col("price").cast("double"),
        F.col("freight_value").cast("double"),
        "order_status",
    )

    items_path = f"{GOLD_BASE}/fact_order_items_scd2"
    fact_items.write.mode("overwrite").parquet(items_path)
    items_count = fact_items.count()
    print(f"  [fact_order_items_scd2] {items_count:,} rows -> {items_path}")

    # ── Verify: null SK check ──
    print(f"\n  --- Null Customer SK Check ---")
    null_orders = fact_orders.filter(F.col("customer_sk").isNull()).count()
    null_items = fact_items.filter(F.col("customer_sk").isNull()).count()
    print(f"  fact_orders_scd2 null customer_sk:      {null_orders}")
    print(f"  fact_order_items_scd2 null customer_sk:  {null_items}")

    # ── Sample: revenue by customer segment (point-in-time) ──
    print(f"\n  --- Revenue by Customer Segment (at time of order) ---")
    (
        fact_items
        .groupBy("customer_segment")
        .agg(
            F.sum("price").alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items"),
        )
        .orderBy(F.desc("revenue"))
        .show(truncate=False)
    )

    # ── Demonstrate historical query ──
    # "What was the segment distribution of orders in Q1 2018 vs Q3 2017?"
    print(f"  --- Segment Distribution: Q3-2017 vs Q1-2018 ---")
    dim_date = spark.read.parquet(f"{GOLD_BASE}/dim_date")
    (
        fact_orders
        .join(dim_date, fact_orders.purchase_date_sk == dim_date.date_sk, "inner")
        .filter(F.col("year_quarter").isin("2017-Q3", "2018-Q1"))
        .groupBy("year_quarter", "customer_segment")
        .agg(F.count("*").alias("orders"), F.sum("order_value").alias("revenue"))
        .orderBy("year_quarter", "customer_segment")
        .show(truncate=False)
    )

    spark.stop()
    print("  Done.")


if __name__ == "__main__":
    main()
