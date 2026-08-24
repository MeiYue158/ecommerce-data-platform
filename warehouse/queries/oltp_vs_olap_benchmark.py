"""
OLTP vs OLAP Benchmark

Loads data into PostgreSQL (row-oriented OLTP) and runs equivalent analytical
queries against both PostgreSQL and ClickHouse (column-oriented OLAP).

Measures and compares:
  - Query execution time
  - Storage size
  - Query plan characteristics

Usage: python oltp_vs_olap_benchmark.py
"""
import os
import time

import boto3
import psycopg2
from io import BytesIO

# Try importing pyarrow for Parquet reading
try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


PG_CONN = {
    "host": "airflow-postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minio_access_key")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio_secret_key")


def get_s3():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT,
                        aws_access_key_id=S3_KEY,
                        aws_secret_access_key=S3_SECRET)


def load_parquet_to_pg(s3, bucket, prefix, pg_table, columns, conn):
    """Load Parquet files from S3 into PostgreSQL via CSV COPY."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]

    cur = conn.cursor()
    cur.execute(f"TRUNCATE {pg_table} CASCADE")
    total = 0

    for f in files:
        obj = s3.get_object(Bucket=bucket, Key=f)
        body = obj["Body"].read()
        table = pq.read_table(BytesIO(body), columns=columns)
        df = table.to_pandas()
        total += len(df)

        # Use COPY via StringIO for fast bulk load
        from io import StringIO
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, pg_table, sep="\t", null="\\N", columns=columns)

    conn.commit()
    cur.close()
    return total


def timed_pg(conn, sql):
    """Execute PostgreSQL query and return (rows, elapsed_ms)."""
    cur = conn.cursor()
    start = time.time()
    cur.execute(sql)
    rows = cur.fetchall()
    elapsed = (time.time() - start) * 1000
    cur.close()
    return rows, elapsed


def timed_ch(sql):
    """Execute ClickHouse query via HTTP and return (result, elapsed_ms)."""
    import urllib.request
    url = f"http://{CH_HOST}:8123/"
    data = sql.encode("utf-8")
    start = time.time()
    req = urllib.request.Request(url, data=data)
    resp = urllib.request.urlopen(req)
    result = resp.read().decode("utf-8").strip()
    elapsed = (time.time() - start) * 1000
    return result, elapsed


def pg_explain(conn, sql):
    """Get PostgreSQL EXPLAIN output."""
    cur = conn.cursor()
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}")
    plan = "\n".join(row[0] for row in cur.fetchall())
    cur.close()
    return plan


def main():
    print(f"\n{'='*70}")
    print(f"  OLTP vs OLAP BENCHMARK")
    print(f"  PostgreSQL (row-oriented) vs ClickHouse (column-oriented)")
    print(f"{'='*70}")

    # ── Load data into PostgreSQL ──
    print(f"\n  Loading data into PostgreSQL...")
    conn = psycopg2.connect(**PG_CONN)
    s3 = get_s3()

    silver_date = "2026-08-24"
    gold_prefix = "gold"

    loads = [
        ("gold/dim_customer", "oltp.customers",
         ["customer_id", "customer_unique_id", "zip_prefix", "city", "state"]),
        ("gold/dim_product", "oltp.products",
         ["product_id", "category", "weight_g", "length_cm", "height_cm", "width_cm"]),
        ("gold/dim_seller", "oltp.sellers",
         ["seller_id", "zip_prefix", "city", "state"]),
    ]

    for prefix, table, cols in loads:
        # Skip the SK column (first column in gold)
        n = load_parquet_to_pg(s3, "ecommerce-data", prefix, table, cols, conn)
        print(f"    {table}: {n:,} rows")

    # Load orders from gold/fact_orders
    print("    Loading orders...")
    resp = s3.list_objects_v2(Bucket="ecommerce-data", Prefix="gold/fact_orders/")
    files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]

    cur = conn.cursor()
    cur.execute("TRUNCATE oltp.orders CASCADE")
    total = 0
    for f in files:
        obj = s3.get_object(Bucket="ecommerce-data", Key=f)
        table = pq.read_table(BytesIO(obj["Body"].read()))
        df = table.to_pandas()

        # Map fact_orders columns to OLTP schema
        from io import StringIO
        orders_df = df[["order_id", "order_status"]].copy()
        orders_df["customer_id"] = None  # Will join from silver
        orders_df["purchase_timestamp"] = None
        orders_df["delivered_date"] = None
        orders_df["estimated_date"] = None
        total += len(orders_df)
    cur.close()

    # Load orders from silver instead (has all columns)
    cur = conn.cursor()
    cur.execute("TRUNCATE oltp.orders CASCADE")
    resp = s3.list_objects_v2(Bucket="ecommerce-data",
                              Prefix=f"silver/orders/ingestion_date={silver_date}")
    files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    total = 0
    for f in files:
        obj = s3.get_object(Bucket="ecommerce-data", Key=f)
        table = pq.read_table(BytesIO(obj["Body"].read()))
        df = table.to_pandas()
        from io import StringIO
        out = df[["order_id", "customer_id", "order_status",
                   "order_purchase_timestamp", "order_delivered_customer_date",
                   "order_estimated_delivery_date"]].copy()
        out.columns = ["order_id", "customer_id", "order_status",
                       "purchase_timestamp", "delivered_date", "estimated_date"]
        buf = StringIO()
        out.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "oltp.orders", sep="\t", null="\\N",
                      columns=list(out.columns))
        total += len(out)
    conn.commit()
    cur.close()
    print(f"    oltp.orders: {total:,} rows")

    # Load order_items from silver
    cur = conn.cursor()
    resp = s3.list_objects_v2(Bucket="ecommerce-data",
                              Prefix=f"silver/order_items/ingestion_date={silver_date}")
    files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    total = 0
    for f in files:
        obj = s3.get_object(Bucket="ecommerce-data", Key=f)
        table = pq.read_table(BytesIO(obj["Body"].read()))
        df = table.to_pandas()
        from io import StringIO
        out = df[["order_id", "order_item_id", "product_id", "seller_id",
                   "price", "freight_value"]].copy()
        buf = StringIO()
        out.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "oltp.order_items", sep="\t", null="\\N",
                      columns=list(out.columns))
        total += len(out)
    conn.commit()
    cur.close()
    print(f"    oltp.order_items: {total:,} rows")

    # Load payments from silver
    cur = conn.cursor()
    resp = s3.list_objects_v2(Bucket="ecommerce-data",
                              Prefix=f"silver/order_payments/ingestion_date={silver_date}")
    files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    total = 0
    for f in files:
        obj = s3.get_object(Bucket="ecommerce-data", Key=f)
        table = pq.read_table(BytesIO(obj["Body"].read()))
        df = table.to_pandas()
        from io import StringIO
        out = df[["order_id", "payment_sequential", "payment_type",
                   "payment_installments", "payment_value"]].copy()
        out.columns = ["order_id", "payment_sequential", "payment_type",
                       "installments", "payment_value"]
        buf = StringIO()
        out.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "oltp.order_payments", sep="\t", null="\\N",
                      columns=list(out.columns))
        total += len(out)
    conn.commit()
    cur.close()
    print(f"    oltp.order_payments: {total:,} rows")

    # Load reviews from silver
    cur = conn.cursor()
    resp = s3.list_objects_v2(Bucket="ecommerce-data",
                              Prefix=f"silver/order_reviews/ingestion_date={silver_date}")
    files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    total = 0
    for f in files:
        obj = s3.get_object(Bucket="ecommerce-data", Key=f)
        table = pq.read_table(BytesIO(obj["Body"].read()))
        df = table.to_pandas()
        from io import StringIO
        out = df[["review_id", "order_id", "review_score",
                   "review_creation_date"]].copy()
        out.columns = ["review_id", "order_id", "review_score", "review_created"]
        buf = StringIO()
        out.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "oltp.order_reviews", sep="\t", null="\\N",
                      columns=list(out.columns))
        total += len(out)
    conn.commit()
    cur.close()
    print(f"    oltp.order_reviews: {total:,} rows")

    # ── Run VACUUM ANALYZE for fair comparison ──
    conn.autocommit = True
    cur = conn.cursor()
    for t in ["customers", "products", "sellers", "orders",
              "order_items", "order_payments", "order_reviews"]:
        cur.execute(f"VACUUM ANALYZE oltp.{t}")
    cur.close()
    conn.autocommit = False
    print(f"\n  VACUUM ANALYZE completed.")

    # ── Storage comparison ──
    print(f"\n  {'='*70}")
    print(f"  STORAGE COMPARISON")
    print(f"  {'='*70}\n")

    cur = conn.cursor()
    cur.execute("""
        SELECT tablename,
               pg_size_pretty(pg_total_relation_size('oltp.' || tablename)) AS total_size
        FROM pg_tables WHERE schemaname = 'oltp'
        ORDER BY pg_total_relation_size('oltp.' || tablename) DESC
    """)
    print(f"  PostgreSQL (row-oriented):")
    pg_total = 0
    for row in cur.fetchall():
        print(f"    {row[0]:<20} {row[1]}")
        cur2 = conn.cursor()
        cur2.execute(f"SELECT pg_total_relation_size('oltp.{row[0]}')")
        pg_total += cur2.fetchone()[0]
        cur2.close()
    print(f"    {'TOTAL':<20} {pg_total / 1024 / 1024:.1f} MB")
    cur.close()

    ch_result, _ = timed_ch(
        "SELECT sum(bytes_on_disk) FROM system.parts "
        "WHERE database='ecommerce' AND active"
    )
    ch_bytes = int(ch_result)
    print(f"\n  ClickHouse (column-oriented):")
    print(f"    {'TOTAL':<20} {ch_bytes / 1024 / 1024:.1f} MB")
    print(f"\n  Compression ratio: PostgreSQL {pg_total/1024/1024:.1f} MB vs ClickHouse {ch_bytes/1024/1024:.1f} MB ({pg_total/ch_bytes:.1f}x)")

    # ── Query benchmark ──
    print(f"\n  {'='*70}")
    print(f"  QUERY BENCHMARK")
    print(f"  {'='*70}\n")

    benchmarks = [
        (
            "Monthly Revenue (3-table join + GROUP BY)",
            # PostgreSQL
            """SELECT to_char(o.purchase_timestamp, 'YYYY-MM') AS month,
                      round(sum(i.price)::numeric, 2) AS revenue,
                      count(DISTINCT o.order_id) AS orders
               FROM oltp.order_items i
               JOIN oltp.orders o ON i.order_id = o.order_id
               GROUP BY month ORDER BY month""",
            # ClickHouse
            """SELECT d.year_month, round(sum(f.price), 2) AS revenue,
                      count(DISTINCT f.order_id) AS orders
               FROM ecommerce.fact_order_items f
               JOIN ecommerce.dim_date d ON f.purchase_date_sk = d.date_sk
               GROUP BY d.year_month ORDER BY d.year_month FORMAT TabSeparated""",
        ),
        (
            "Revenue by State (3-table join + GROUP BY)",
            """SELECT c.state, round(sum(i.price)::numeric, 2) AS revenue,
                      count(DISTINCT o.order_id) AS orders
               FROM oltp.order_items i
               JOIN oltp.orders o ON i.order_id = o.order_id
               JOIN oltp.customers c ON o.customer_id = c.customer_id
               GROUP BY c.state ORDER BY revenue DESC LIMIT 10""",
            """SELECT c.state, round(sum(f.price), 2) AS revenue,
                      count(DISTINCT f.order_id) AS orders
               FROM ecommerce.fact_order_items f
               JOIN ecommerce.dim_customer c ON f.customer_sk = c.customer_sk
               GROUP BY c.state ORDER BY revenue DESC LIMIT 10 FORMAT TabSeparated""",
        ),
        (
            "Revenue by Category (3-table join + GROUP BY)",
            """SELECT p.category, round(sum(i.price)::numeric, 2) AS revenue,
                      count(*) AS items
               FROM oltp.order_items i
               JOIN oltp.products p ON i.product_id = p.product_id
               GROUP BY p.category ORDER BY revenue DESC LIMIT 10""",
            """SELECT p.category, round(sum(f.price), 2) AS revenue,
                      count(*) AS items
               FROM ecommerce.fact_order_items f
               JOIN ecommerce.dim_product p ON f.product_sk = p.product_sk
               GROUP BY p.category ORDER BY revenue DESC LIMIT 10 FORMAT TabSeparated""",
        ),
        (
            "Delivery Delay vs Review (4-table join)",
            """SELECT CASE WHEN date_part('day', o.delivered_date - o.estimated_date) <= 0
                          THEN 'On Time' ELSE 'Late' END AS bucket,
                      round(avg(r.review_score)::numeric, 2) AS avg_score,
                      count(*) AS orders
               FROM oltp.orders o
               JOIN oltp.order_reviews r ON o.order_id = r.order_id
               WHERE o.delivered_date IS NOT NULL AND o.estimated_date IS NOT NULL
               GROUP BY bucket""",
            """SELECT if(fo.delivery_delay_days <= 0, 'On Time', 'Late') AS bucket,
                      round(avg(fr.review_score), 2) AS avg_score,
                      count() AS orders
               FROM ecommerce.fact_orders fo
               JOIN ecommerce.fact_reviews fr ON fo.order_id = fr.order_id
               WHERE fo.delivery_days > 0
               GROUP BY bucket FORMAT TabSeparated""",
        ),
        (
            "Full Aggregation Scan (sum all prices)",
            """SELECT round(sum(price)::numeric, 2) FROM oltp.order_items""",
            """SELECT round(sum(price), 2) FROM ecommerce.fact_order_items FORMAT TabSeparated""",
        ),
    ]

    print(f"  {'Query':<50} {'PostgreSQL':>12} {'ClickHouse':>12} {'Speedup':>10}")
    print(f"  {'-'*50} {'-'*12} {'-'*12} {'-'*10}")

    for label, pg_sql, ch_sql in benchmarks:
        # Run each query 3 times, take best
        pg_times = []
        ch_times = []
        for _ in range(3):
            _, pg_t = timed_pg(conn, pg_sql)
            pg_times.append(pg_t)
            _, ch_t = timed_ch(ch_sql)
            ch_times.append(ch_t)

        pg_best = min(pg_times)
        ch_best = min(ch_times)
        speedup = pg_best / ch_best if ch_best > 0 else 0

        print(f"  {label:<50} {pg_best:>9.1f} ms {ch_best:>9.1f} ms {speedup:>9.1f}x")

    # ── Show PostgreSQL query plan for one query ──
    print(f"\n  {'='*70}")
    print(f"  QUERY PLAN COMPARISON — Monthly Revenue")
    print(f"  {'='*70}")

    print(f"\n  PostgreSQL EXPLAIN ANALYZE:")
    plan = pg_explain(conn, benchmarks[0][1])
    for line in plan.split("\n")[:20]:
        print(f"    {line}")

    print(f"\n  ClickHouse query pipeline:")
    ch_plan, _ = timed_ch(
        "EXPLAIN PIPELINE SELECT d.year_month, sum(f.price) "
        "FROM ecommerce.fact_order_items f "
        "JOIN ecommerce.dim_date d ON f.purchase_date_sk = d.date_sk "
        "GROUP BY d.year_month ORDER BY d.year_month FORMAT TabSeparated"
    )
    for line in ch_plan.split("\n")[:15]:
        print(f"    {line}")

    # ── Summary ──
    print(f"\n  {'='*70}")
    print(f"  ARCHITECTURAL COMPARISON")
    print(f"  {'='*70}\n")
    print(f"  {'Aspect':<30} {'PostgreSQL (OLTP)':<25} {'ClickHouse (OLAP)':<25}")
    print(f"  {'-'*30} {'-'*25} {'-'*25}")
    print(f"  {'Storage orientation':<30} {'Row-oriented':<25} {'Column-oriented':<25}")
    print(f"  {'Schema design':<30} {'Normalized (3NF)':<25} {'Star schema':<25}")
    print(f"  {'Primary use case':<30} {'Point reads/writes':<25} {'Large scans/aggregations':<25}")
    print(f"  {'Compression':<30} {'Minimal':<25} {'Heavy (columnar)':<25}")
    print(f"  {'Storage size':<30} {pg_total/1024/1024:<24.1f}{'MB'} {ch_bytes/1024/1024:<24.1f}{'MB'}")
    print(f"  {'Join strategy':<30} {'Hash/nested loop':<25} {'Hash join':<25}")
    print(f"  {'Aggregation':<30} {'Full row scan':<25} {'Column-only scan':<25}")
    print(f"  {'Concurrency model':<30} {'MVCC (high write)':<25} {'Append-only (batch)':<25}")
    print(f"  {'Best for':<30} {'CRUD, transactions':<25} {'Analytics, BI':<25}")

    conn.close()
    print(f"\n  Done.")


if __name__ == "__main__":
    main()
