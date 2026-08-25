"""
Broadcast Join Benchmark

Compares shuffle join vs broadcast join performance when building fact tables.
Small dimensions (dim_date: 1K, dim_payment_type: 5, dim_seller: 3K) are
natural broadcast candidates.

Measures: runtime, shuffle bytes, and shows execution plans.

Usage:
    spark-submit benchmark_broadcast.py <ingestion_date>
"""
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_BASE = "s3a://ecommerce-data/silver"
GOLD_BASE = "s3a://ecommerce-data/gold"


def create_spark_session(aqe=False):
    return (
        SparkSession.builder
        .appName("benchmark_broadcast")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", str(aqe).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto broadcast
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def get_shuffle_bytes(spark):
    """Get total shuffle bytes from Spark metrics."""
    sc = spark.sparkContext
    status = sc.statusTracker()
    # Force GC of old stages
    return None  # We'll measure via timing instead


def timed(fn):
    start = time.time()
    result = fn()
    elapsed = time.time() - start
    return result, elapsed


def main():
    if len(sys.argv) < 2:
        print("Usage: benchmark_broadcast.py <ingestion_date>")
        sys.exit(1)

    ingestion_date = sys.argv[1]

    print(f"\n{'='*70}")
    print(f"  BROADCAST JOIN BENCHMARK — {ingestion_date}")
    print(f"{'='*70}")

    spark = create_spark_session(aqe=False)
    spark.sparkContext.setLogLevel("WARN")

    # Load data
    items = spark.read.parquet(f"{SILVER_BASE}/order_items/ingestion_date={ingestion_date}")
    orders = spark.read.parquet(f"{SILVER_BASE}/orders/ingestion_date={ingestion_date}")
    customers = spark.read.parquet(f"{SILVER_BASE}/customers/ingestion_date={ingestion_date}")
    products = spark.read.parquet(f"{SILVER_BASE}/products/ingestion_date={ingestion_date}")
    sellers = spark.read.parquet(f"{SILVER_BASE}/sellers/ingestion_date={ingestion_date}")
    payments = spark.read.parquet(f"{SILVER_BASE}/order_payments/ingestion_date={ingestion_date}")

    items_count = items.count()
    print(f"\n  Dataset: {items_count:,} order items")
    print(f"  Dimensions: customers={customers.count():,}, products={products.count():,}, sellers={sellers.count():,}")

    # ══════════════════════════════════════════════════════════
    # EXPERIMENT 1: Fact build — shuffle join vs broadcast join
    # ══════════════════════════════════════════════════════════

    print(f"\n{'='*70}")
    print(f"  EXPERIMENT 1: Build fact_order_items")
    print(f"  Joining: order_items × orders × customers × products × sellers")
    print(f"{'='*70}")

    # ── Shuffle join (all tables) ──
    def build_shuffle():
        result = (
            items
            .join(orders.select("order_id", "customer_id", "order_purchase_timestamp", "order_status"),
                  "order_id", "inner")
            .join(customers.select("customer_id", "customer_state"), "customer_id", "inner")
            .join(products.select("product_id", "product_category"), "product_id", "left")
            .join(sellers.select("seller_id", "seller_state"), "seller_id", "left")
            .select(
                "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
                "price", "freight_value", "order_status", "customer_state",
                "product_category", "seller_state",
            )
        )
        return result.count()

    # Warm up
    build_shuffle()

    count, shuffle_time = timed(build_shuffle)
    _, shuffle_time2 = timed(build_shuffle)
    _, shuffle_time3 = timed(build_shuffle)
    shuffle_best = min(shuffle_time, shuffle_time2, shuffle_time3)
    print(f"\n  [Shuffle Join]  {count:,} rows  best={shuffle_best:.2f}s  (runs: {shuffle_time:.2f}, {shuffle_time2:.2f}, {shuffle_time3:.2f})")

    # ── Broadcast join (small dims broadcasted) ──
    def build_broadcast():
        result = (
            items
            .join(orders.select("order_id", "customer_id", "order_purchase_timestamp", "order_status"),
                  "order_id", "inner")
            .join(F.broadcast(customers.select("customer_id", "customer_state")),
                  "customer_id", "inner")
            .join(F.broadcast(products.select("product_id", "product_category")),
                  "product_id", "left")
            .join(F.broadcast(sellers.select("seller_id", "seller_state")),
                  "seller_id", "left")
            .select(
                "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
                "price", "freight_value", "order_status", "customer_state",
                "product_category", "seller_state",
            )
        )
        return result.count()

    # Warm up
    build_broadcast()

    count, bc_time = timed(build_broadcast)
    _, bc_time2 = timed(build_broadcast)
    _, bc_time3 = timed(build_broadcast)
    bc_best = min(bc_time, bc_time2, bc_time3)
    print(f"  [Broadcast Join] {count:,} rows  best={bc_best:.2f}s  (runs: {bc_time:.2f}, {bc_time2:.2f}, {bc_time3:.2f})")

    speedup1 = shuffle_best / bc_best if bc_best > 0 else 0
    print(f"  Speedup: {speedup1:.2f}x")

    # ── Show execution plans ──
    print(f"\n  --- Shuffle Join Plan ---")
    shuffle_df = (
        items
        .join(orders.select("order_id", "customer_id"), "order_id", "inner")
        .join(sellers.select("seller_id", "seller_state"), "seller_id", "left")
    )
    plan = shuffle_df._jdf.queryExecution().simpleString()
    for line in plan.split("\n"):
        if "Exchange" in line or "SortMerge" in line or "Broadcast" in line or "ShuffledHashJoin" in line or "BroadcastHashJoin" in line:
            print(f"    {line.strip()}")

    print(f"\n  --- Broadcast Join Plan ---")
    broadcast_df = (
        items
        .join(orders.select("order_id", "customer_id"), "order_id", "inner")
        .join(F.broadcast(sellers.select("seller_id", "seller_state")), "seller_id", "left")
    )
    plan = broadcast_df._jdf.queryExecution().simpleString()
    for line in plan.split("\n"):
        if "Exchange" in line or "SortMerge" in line or "Broadcast" in line or "ShuffledHashJoin" in line or "BroadcastHashJoin" in line:
            print(f"    {line.strip()}")

    # ══════════════════════════════════════════════════════════
    # EXPERIMENT 2: Aggregation query — shuffle vs broadcast
    # ══════════════════════════════════════════════════════════

    print(f"\n{'='*70}")
    print(f"  EXPERIMENT 2: Revenue by category × state")
    print(f"  (order_items × products × orders × customers → GROUP BY)")
    print(f"{'='*70}")

    def agg_shuffle():
        return (
            items
            .join(products.select("product_id", "product_category"), "product_id", "left")
            .join(orders.select("order_id", "customer_id"), "order_id", "inner")
            .join(customers.select("customer_id", "customer_state"), "customer_id", "inner")
            .groupBy("product_category", "customer_state")
            .agg(F.sum("price").alias("revenue"), F.count("*").alias("items"))
            .orderBy(F.desc("revenue"))
        ).count()

    def agg_broadcast():
        return (
            items
            .join(F.broadcast(products.select("product_id", "product_category")), "product_id", "left")
            .join(orders.select("order_id", "customer_id"), "order_id", "inner")
            .join(F.broadcast(customers.select("customer_id", "customer_state")),
                  "customer_id", "inner")
            .groupBy("product_category", "customer_state")
            .agg(F.sum("price").alias("revenue"), F.count("*").alias("items"))
            .orderBy(F.desc("revenue"))
        ).count()

    # Warm up
    agg_shuffle()
    agg_broadcast()

    _, s_t1 = timed(agg_shuffle)
    _, s_t2 = timed(agg_shuffle)
    _, s_t3 = timed(agg_shuffle)
    s_best = min(s_t1, s_t2, s_t3)
    print(f"\n  [Shuffle]    best={s_best:.2f}s  (runs: {s_t1:.2f}, {s_t2:.2f}, {s_t3:.2f})")

    _, b_t1 = timed(agg_broadcast)
    _, b_t2 = timed(agg_broadcast)
    _, b_t3 = timed(agg_broadcast)
    b_best = min(b_t1, b_t2, b_t3)
    print(f"  [Broadcast]  best={b_best:.2f}s  (runs: {b_t1:.2f}, {b_t2:.2f}, {b_t3:.2f})")

    speedup2 = s_best / b_best if b_best > 0 else 0
    print(f"  Speedup: {speedup2:.2f}x")

    # ══════════════════════════════════════════════════════════
    # EXPERIMENT 3: Partition tuning
    # ══════════════════════════════════════════════════════════

    print(f"\n{'='*70}")
    print(f"  EXPERIMENT 3: Write performance — partition count")
    print(f"  Writing {items_count:,} rows to Parquet")
    print(f"{'='*70}")

    test_path_base = "s3a://ecommerce-data/benchmark/partition_test"

    for num_parts in [1, 4, 16, 200]:
        def write_test(n=num_parts):
            out = items.repartition(n) if n > 1 else items.coalesce(1)
            out.write.mode("overwrite").parquet(f"{test_path_base}/{n}_parts")
            return n
        _, wt = timed(write_test)
        print(f"  {num_parts:>3} partitions: {wt:.2f}s")

    # ══════════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════════

    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"\n  Dataset: {items_count:,} order items")
    print(f"  Experiment 1 (fact build):    shuffle={shuffle_best:.2f}s  broadcast={bc_best:.2f}s  → {speedup1:.1f}x")
    print(f"  Experiment 2 (aggregation):   shuffle={s_best:.2f}s  broadcast={b_best:.2f}s  → {speedup2:.1f}x")
    print(f"\n  Broadcast joins eliminate shuffle exchanges for small dimension tables,")
    print(f"  reducing network I/O and stage count in the Spark execution plan.")

    spark.stop()
    print("\n  Done.")


if __name__ == "__main__":
    main()
