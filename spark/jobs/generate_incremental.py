"""
Incremental Data Generator

Generates synthetic daily batch data that mimics operational source system extracts.
Uses the real dataset's distributions for realistic data:
  - State/city distribution from real customers
  - Category/price distribution from real products
  - Seller concentration from real sellers
  - Payment type distribution from real payments
  - Order size distribution from real order items

Output:
  /opt/data/incremental/{batch_date}/
    new_orders.csv
    new_order_items.csv
    new_order_payments.csv
    new_order_reviews.csv
    customer_updates.csv
    late_orders.csv          (orders with past purchase dates)
    late_order_items.csv

Usage:
    spark-submit generate_incremental.py <batch_date> [--orders N] [--late N] [--updates N]
"""
import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


SILVER_BASE = "s3a://ecommerce-data/silver"
OUTPUT_BASE = "/opt/data/incremental"

DEFAULT_NEW_ORDERS = 500
DEFAULT_LATE_ORDERS = 25
DEFAULT_CUSTOMER_UPDATES = 50


def create_spark_session():
    return (
        SparkSession.builder
        .appName("generate_incremental")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_silver(spark, table, ingestion_date="2026-08-24"):
    return spark.read.parquet(f"{SILVER_BASE}/{table}/ingestion_date={ingestion_date}")


class IncrementalGenerator:
    def __init__(self, spark, ingestion_date="2026-08-24"):
        self.spark = spark

        # Load reference data for sampling distributions
        self.customers = read_silver(spark, "customers", ingestion_date).cache()
        self.products = read_silver(spark, "products", ingestion_date).cache()
        self.sellers = read_silver(spark, "sellers", ingestion_date).cache()
        self.payments_ref = read_silver(spark, "order_payments", ingestion_date).cache()
        self.items_ref = read_silver(spark, "order_items", ingestion_date).cache()
        self.reviews_ref = read_silver(spark, "order_reviews", ingestion_date).cache()

        self.customer_count = self.customers.count()
        self.product_count = self.products.count()
        self.seller_count = self.sellers.count()

    def _generate_ids(self, n, prefix="ord"):
        """Generate N rows with unique IDs using Spark."""
        return (
            self.spark.range(0, n)
            .withColumn("order_id", F.expr("uuid()"))
            .drop("id")
        )

    def generate_orders(self, batch_date, count):
        """Generate new orders with purchase dates on or near batch_date."""
        # Sample customers with sequential row numbers for reliable join
        sampled_customers = (
            self.customers
            .select("customer_id")
            .orderBy(F.rand(seed=hash(batch_date) & 0xFFFFFFFF))
            .limit(count)
            .withColumn("_row", F.row_number().over(Window.orderBy(F.lit(1))) - 1)
        )

        # Create order IDs with matching row numbers
        orders = (
            self.spark.range(0, count)
            .withColumnRenamed("id", "_row")
            .withColumn("order_id", F.expr("uuid()"))
        )

        orders = orders.join(sampled_customers, "_row").drop("_row")

        # Generate timestamps: spread across the batch day
        orders = (
            orders
            .withColumn(
                "order_purchase_timestamp",
                F.from_unixtime(
                    F.unix_timestamp(F.lit(batch_date).cast("timestamp"))
                    + (F.rand() * 86400).cast("int")  # random second within day
                ),
            )
            .withColumn("order_status", F.lit("delivered"))
            .withColumn(
                "order_approved_at",
                F.col("order_purchase_timestamp") + F.expr("INTERVAL 1 HOUR") * (F.rand() * 24),
            )
            .withColumn(
                "order_delivered_carrier_date",
                F.col("order_purchase_timestamp") + F.expr("INTERVAL 1 DAY") * (1 + F.rand() * 3).cast("int"),
            )
            .withColumn(
                "order_delivered_customer_date",
                F.col("order_delivered_carrier_date") + F.expr("INTERVAL 1 DAY") * (2 + F.rand() * 10).cast("int"),
            )
            .withColumn(
                "order_estimated_delivery_date",
                F.col("order_purchase_timestamp") + F.expr("INTERVAL 1 DAY") * (7 + F.rand() * 21).cast("int"),
            )
        )

        return orders.select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        )

    def generate_order_items(self, orders_df):
        """Generate 1-3 items per order, sampling from real product/seller/price distributions."""
        # Determine items per order (weighted: 70% 1 item, 20% 2, 10% 3)
        orders_with_items = (
            orders_df
            .select("order_id")
            .withColumn("rand_val", F.rand())
            .withColumn(
                "num_items",
                F.when(F.col("rand_val") < 0.7, 1)
                .when(F.col("rand_val") < 0.9, 2)
                .otherwise(3),
            )
            .drop("rand_val")
        )

        # Explode to one row per item
        items = (
            orders_with_items
            .withColumn("order_item_id", F.explode(F.sequence(F.lit(1), F.col("num_items"))))
            .drop("num_items")
        )

        item_count = items.count()

        # Sample products and sellers using row_number for reliable join
        sampled_products = (
            self.products
            .select("product_id")
            .orderBy(F.rand(seed=42))
            .limit(item_count)
            .withColumn("_row", F.row_number().over(Window.orderBy(F.lit(1))) - 1)
        )

        sampled_sellers = (
            self.sellers
            .select("seller_id")
            .orderBy(F.rand(seed=99))
            .limit(item_count)
            .withColumn("_row", F.row_number().over(Window.orderBy(F.lit(1))) - 1)
        )

        price_stats = self.items_ref.select(
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("std_price"),
            F.avg("freight_value").alias("avg_freight"),
        ).first()

        items = items.withColumn("_row", F.row_number().over(Window.orderBy(F.lit(1))) - 1)
        items = (
            items
            .join(sampled_products, "_row", "left")
            .join(sampled_sellers, "_row", "left")
            .drop("_row")
        )

        # Generate realistic prices and freight
        items = (
            items
            .withColumn(
                "price",
                F.round(F.abs(F.lit(price_stats["avg_price"]) + F.randn() * F.lit(price_stats["std_price"])), 2),
            )
            .withColumn("price", F.greatest("price", F.lit(5.0)))  # minimum R$5
            .withColumn(
                "freight_value",
                F.round(F.abs(F.lit(price_stats["avg_freight"]) + F.randn() * 10), 2),
            )
            .withColumn(
                "shipping_limit_date",
                F.current_timestamp() + F.expr("INTERVAL 7 DAY"),
            )
        )

        return items.select(
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        )

    def generate_payments(self, orders_df, items_df):
        """Generate payment records matching order totals."""
        order_totals = items_df.groupBy("order_id").agg(
            F.sum(F.col("price") + F.col("freight_value")).alias("total"),
        )

        # Payment type distribution from real data
        # credit_card: 74%, boleto: 19%, voucher: 6%, debit_card: 1%
        payments = (
            order_totals
            .withColumn("payment_sequential", F.lit(1))
            .withColumn(
                "payment_type",
                F.when(F.rand() < 0.74, "credit_card")
                .when(F.rand() < 0.93, "boleto")
                .when(F.rand() < 0.99, "voucher")
                .otherwise("debit_card"),
            )
            .withColumn(
                "payment_installments",
                F.when(F.col("payment_type") == "credit_card",
                       (1 + F.rand() * 10).cast("int"))
                .otherwise(F.lit(1)),
            )
            .withColumnRenamed("total", "payment_value")
        )

        return payments.select(
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        )

    def generate_reviews(self, orders_df):
        """Generate reviews for ~80% of orders."""
        reviewed = orders_df.filter(F.rand() < 0.8)

        reviews = (
            reviewed
            .select("order_id")
            .withColumn("review_id", F.expr("uuid()"))
            .withColumn(
                "review_score",
                # Weighted: 57% score 5, 19% score 4, 8% score 3, 3% score 2, 11% score 1
                F.when(F.rand() < 0.57, 5)
                .when(F.rand() < 0.76, 4)
                .when(F.rand() < 0.84, 3)
                .when(F.rand() < 0.87, 2)
                .otherwise(1),
            )
            .withColumn("review_comment_title", F.lit(None).cast("string"))
            .withColumn("review_comment_message", F.lit(None).cast("string"))
            .withColumn("review_creation_date", F.current_timestamp())
            .withColumn(
                "review_answer_timestamp",
                F.current_timestamp() + F.expr("INTERVAL 1 DAY") * (F.rand() * 3).cast("int"),
            )
        )

        return reviews

    def generate_late_orders(self, batch_date, count):
        """Generate orders with purchase dates 1-7 days before batch_date."""
        late = self.generate_orders(batch_date, count)

        # Shift purchase date backward by 1-7 days
        late = (
            late
            .withColumn(
                "days_late",
                (1 + F.rand() * 6).cast("int"),
            )
            .withColumn(
                "order_purchase_timestamp",
                F.col("order_purchase_timestamp") - F.expr("INTERVAL 1 DAY") * F.col("days_late"),
            )
            # Adjust downstream dates too
            .withColumn(
                "order_approved_at",
                F.col("order_purchase_timestamp") + F.expr("INTERVAL 1 HOUR") * (F.rand() * 24),
            )
            .withColumn(
                "order_delivered_carrier_date",
                F.col("order_purchase_timestamp") + F.expr("INTERVAL 1 DAY") * (1 + F.rand() * 3).cast("int"),
            )
            .withColumn(
                "order_delivered_customer_date",
                F.col("order_delivered_carrier_date") + F.expr("INTERVAL 1 DAY") * (2 + F.rand() * 10).cast("int"),
            )
            .drop("days_late")
        )

        return late.select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        )

    def generate_customer_updates(self, count):
        """Generate city changes for existing customers."""
        # Pick random customers
        updated = (
            self.customers
            .orderBy(F.rand())
            .limit(count)
        )

        # Swap city to another city in same state
        other_cities = (
            self.customers
            .select("customer_state", "customer_city").distinct()
            .withColumn(
                "new_city",
                F.lead("customer_city").over(
                    Window.partitionBy("customer_state").orderBy("customer_city")
                ),
            )
            .filter(F.col("new_city").isNotNull())
        )

        updates = (
            updated
            .join(other_cities, ["customer_state", "customer_city"], "left")
            .filter(F.col("new_city").isNotNull())
            .withColumn("customer_city", F.col("new_city"))
            .drop("new_city")
        )

        return updates


def write_csv(df, path):
    """Write DataFrame as single CSV file."""
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    count = df.count()
    print(f"    {path.split('/')[-1]:<30} {count:>8,} rows")
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_date", help="Date to generate data for (YYYY-MM-DD)")
    parser.add_argument("--orders", type=int, default=DEFAULT_NEW_ORDERS)
    parser.add_argument("--late", type=int, default=DEFAULT_LATE_ORDERS)
    parser.add_argument("--updates", type=int, default=DEFAULT_CUSTOMER_UPDATES)
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  INCREMENTAL DATA GENERATION — {args.batch_date}")
    print(f"{'='*60}")
    print(f"  New orders:       {args.orders}")
    print(f"  Late orders:      {args.late}")
    print(f"  Customer updates: {args.updates}")

    gen = IncrementalGenerator(spark)
    output_dir = f"{OUTPUT_BASE}/{args.batch_date}"

    # ── Generate new orders ──
    print(f"\n  Generating new records...")
    new_orders = gen.generate_orders(args.batch_date, args.orders)
    new_items = gen.generate_order_items(new_orders)
    new_payments = gen.generate_payments(new_orders, new_items)
    new_reviews = gen.generate_reviews(new_orders)

    write_csv(new_orders, f"{output_dir}/new_orders")
    write_csv(new_items, f"{output_dir}/new_order_items")
    write_csv(new_payments, f"{output_dir}/new_order_payments")
    write_csv(new_reviews, f"{output_dir}/new_order_reviews")

    # ── Generate late-arriving orders ──
    print(f"\n  Generating late-arriving records...")
    late_orders = gen.generate_late_orders(args.batch_date, args.late)
    late_items = gen.generate_order_items(late_orders)
    late_payments = gen.generate_payments(late_orders, late_items)

    write_csv(late_orders, f"{output_dir}/late_orders")
    write_csv(late_items, f"{output_dir}/late_order_items")
    write_csv(late_payments, f"{output_dir}/late_order_payments")

    # ── Generate customer updates ──
    print(f"\n  Generating customer updates...")
    updates = gen.generate_customer_updates(args.updates)
    write_csv(updates, f"{output_dir}/customer_updates")

    # ── Summary ──
    print(f"\n  {'='*50}")
    print(f"  Output directory: {output_dir}")

    # Show late order date distribution
    print(f"\n  --- Late Order Purchase Dates ---")
    late_orders.groupBy(
        F.col("order_purchase_timestamp").cast("date").alias("purchase_date")
    ).count().orderBy("purchase_date").show(truncate=False)

    spark.stop()
    print("  Done.")


if __name__ == "__main__":
    main()
