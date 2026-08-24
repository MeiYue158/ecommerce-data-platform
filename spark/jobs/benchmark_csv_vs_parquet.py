"""
Phase 3: CSV vs Parquet Performance Benchmark

Compares CSV and Parquet formats across multiple dimensions:
  1. Storage size
  2. Full table scan
  3. Column pruning (select subset of columns)
  4. Predicate pushdown (filtered reads)
  5. Aggregation

Usage:
    spark-submit benchmark_csv_vs_parquet.py
"""
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


MINIO_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minio_access_key",
    "spark.hadoop.fs.s3a.secret.key": "minio_secret_key",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

SEED_DIR = "/opt/data/seed"
BRONZE_BASE = "s3a://ecommerce-data/bronze"
INGESTION_DATE = "2026-08-24"

# Tables to benchmark: (name, csv_file, interesting_columns, filter_column, filter_value)
BENCHMARKS = [
    {
        "name": "orders",
        "csv": f"{SEED_DIR}/olist_orders_dataset.csv",
        "parquet": f"{BRONZE_BASE}/orders/ingestion_date={INGESTION_DATE}",
        "select_cols": ["order_id", "order_status", "order_purchase_timestamp"],
        "filter_col": "order_status",
        "filter_val": "delivered",
        "agg_col": "order_status",
    },
    {
        "name": "geolocation",
        "csv": f"{SEED_DIR}/olist_geolocation_dataset.csv",
        "parquet": f"{BRONZE_BASE}/geolocation/ingestion_date={INGESTION_DATE}",
        "select_cols": ["geolocation_zip_code_prefix", "geolocation_state"],
        "filter_col": "geolocation_state",
        "filter_val": "SP",
        "agg_col": "geolocation_state",
    },
    {
        "name": "order_items",
        "csv": f"{SEED_DIR}/olist_order_items_dataset.csv",
        "parquet": f"{BRONZE_BASE}/order_items/ingestion_date={INGESTION_DATE}",
        "select_cols": ["order_id", "price"],
        "filter_col": "price",
        "filter_val": 100.0,
        "agg_col": "seller_id",
    },
]


def timed(fn):
    """Run fn, return (result, elapsed_seconds). Forces materialization."""
    from pyspark.sql import DataFrame
    start = time.time()
    result = fn()
    if isinstance(result, DataFrame):
        count = result.count()
    elif isinstance(result, (list, tuple)):
        count = len(result)
    elif isinstance(result, int):
        count = result
    else:
        count = result
    elapsed = time.time() - start
    return count, elapsed


def get_dir_size_bytes(path):
    """Get total size of files in a local directory."""
    total = 0
    if os.path.isfile(path):
        return os.path.getsize(path)
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            total += os.path.getsize(os.path.join(dirpath, f))
    return total


def fmt_size(nbytes):
    """Format bytes as human-readable."""
    for unit in ["B", "KB", "MB", "GB"]:
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def fmt_time(seconds):
    return f"{seconds:.3f}s"


def run_benchmark(spark, bench):
    name = bench["name"]
    csv_path = bench["csv"]
    parquet_path = bench["parquet"]
    select_cols = bench["select_cols"]
    filter_col = bench["filter_col"]
    filter_val = bench["filter_val"]
    agg_col = bench["agg_col"]

    print(f"\n{'='*70}")
    print(f"  TABLE: {name}")
    print(f"{'='*70}")

    # ── 1. Storage Size ──
    csv_size = get_dir_size_bytes(csv_path)
    # For Parquet in MinIO, we measure via Spark (can't use os.path)
    # Instead, read and check plan
    print(f"\n  CSV file size:     {fmt_size(csv_size)}")

    # ── 2. Full Scan ──
    # CSV
    csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    _, csv_full_time = timed(lambda: csv_df.count())
    # again to get warm timing
    csv_count, csv_full_time = timed(lambda: csv_df.count())

    # Parquet (exclude metadata columns)
    pq_df = spark.read.parquet(parquet_path)
    data_cols = [c for c in pq_df.columns if not c.startswith("_")]
    pq_df = pq_df.select(data_cols)
    _, pq_full_time = timed(lambda: pq_df.count())
    pq_count, pq_full_time = timed(lambda: pq_df.count())

    print(f"\n  [Full Scan]")
    print(f"    CSV:     {csv_count:>10,} rows  {fmt_time(csv_full_time)}")
    print(f"    Parquet: {pq_count:>10,} rows  {fmt_time(pq_full_time)}")
    print(f"    Speedup: {csv_full_time / pq_full_time:.1f}x" if pq_full_time > 0 else "")

    # ── 3. Column Pruning ──
    csv_sel = csv_df.select(select_cols)
    _, csv_sel_time = timed(lambda: csv_sel.count())
    _, csv_sel_time = timed(lambda: csv_sel.count())

    pq_sel = spark.read.parquet(parquet_path).select(select_cols)
    _, pq_sel_time = timed(lambda: pq_sel.count())
    _, pq_sel_time = timed(lambda: pq_sel.count())

    print(f"\n  [Column Pruning] select {select_cols}")
    print(f"    CSV:     {fmt_time(csv_sel_time)}")
    print(f"    Parquet: {fmt_time(pq_sel_time)}")
    print(f"    Speedup: {csv_sel_time / pq_sel_time:.1f}x" if pq_sel_time > 0 else "")

    # ── 4. Predicate Pushdown ──
    if isinstance(filter_val, str):
        csv_filt = csv_df.filter(F.col(filter_col) == filter_val)
        pq_filt = spark.read.parquet(parquet_path).filter(F.col(filter_col) == filter_val)
    else:
        csv_filt = csv_df.filter(F.col(filter_col) > filter_val)
        pq_filt = spark.read.parquet(parquet_path).filter(F.col(filter_col) > filter_val)

    csv_filt_count, csv_filt_time = timed(lambda: csv_filt.count())
    csv_filt_count, csv_filt_time = timed(lambda: csv_filt.count())

    pq_filt_count, pq_filt_time = timed(lambda: pq_filt.count())
    pq_filt_count, pq_filt_time = timed(lambda: pq_filt.count())

    filter_desc = f"{filter_col} == '{filter_val}'" if isinstance(filter_val, str) else f"{filter_col} > {filter_val}"
    print(f"\n  [Predicate Pushdown] {filter_desc}")
    print(f"    CSV:     {csv_filt_count:>10,} rows  {fmt_time(csv_filt_time)}")
    print(f"    Parquet: {pq_filt_count:>10,} rows  {fmt_time(pq_filt_time)}")
    print(f"    Speedup: {csv_filt_time / pq_filt_time:.1f}x" if pq_filt_time > 0 else "")

    # ── 5. Aggregation ──
    csv_agg = csv_df.groupBy(agg_col).agg(F.count("*").alias("cnt"))
    _, csv_agg_time = timed(lambda: csv_agg.collect())
    _, csv_agg_time = timed(lambda: csv_agg.collect())

    pq_agg = spark.read.parquet(parquet_path).groupBy(agg_col).agg(F.count("*").alias("cnt"))
    _, pq_agg_time = timed(lambda: pq_agg.collect())
    _, pq_agg_time = timed(lambda: pq_agg.collect())

    print(f"\n  [Aggregation] GROUP BY {agg_col}")
    print(f"    CSV:     {fmt_time(csv_agg_time)}")
    print(f"    Parquet: {fmt_time(pq_agg_time)}")
    print(f"    Speedup: {csv_agg_time / pq_agg_time:.1f}x" if pq_agg_time > 0 else "")

    # ── 6. Parquet physical plan (show predicate pushdown) ──
    print(f"\n  [Execution Plan - Parquet filtered read]")
    pq_filt.explain(True)


def main():
    builder = SparkSession.builder.appName("csv_vs_parquet_benchmark")
    for k, v in MINIO_CONF.items():
        builder = builder.config(k, v)
    # Disable adaptive query execution for consistent benchmarks
    builder = builder.config("spark.sql.adaptive.enabled", "false")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 70)
    print("  CSV vs PARQUET BENCHMARK")
    print("=" * 70)

    for bench in BENCHMARKS:
        run_benchmark(spark, bench)

    # ── Storage comparison summary ──
    print(f"\n{'='*70}")
    print(f"  STORAGE SIZE COMPARISON")
    print(f"{'='*70}\n")
    print(f"  {'Table':<25} {'CSV':>12} {'Parquet':>12} {'Ratio':>8}")
    print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*8}")

    for bench in BENCHMARKS:
        csv_size = get_dir_size_bytes(bench["csv"])
        # Read parquet and estimate size from input metrics
        pq_df = spark.read.parquet(bench["parquet"])
        pq_df.count()  # force read
        # Get input bytes from Spark SQL metrics
        plan = pq_df._jdf.queryExecution().executedPlan().toString()
        # Approximate: write parquet locally to measure
        local_pq = f"/tmp/bench_{bench['name']}.parquet"
        data_cols = [c for c in pq_df.columns if not c.startswith("_")]
        pq_df.select(data_cols).write.mode("overwrite").parquet(local_pq)
        pq_size = get_dir_size_bytes(local_pq)
        ratio = csv_size / pq_size if pq_size > 0 else 0
        print(f"  {bench['name']:<25} {fmt_size(csv_size):>12} {fmt_size(pq_size):>12} {ratio:>7.1f}x")

    spark.stop()
    print("\nBenchmark complete.")


if __name__ == "__main__":
    main()
