"""
Pipeline Observability Collector

Collects metrics from multiple sources and writes to ClickHouse:
  1. Airflow metadata DB → pipeline_runs, task_metrics
  2. MinIO → data_freshness (partition sizes, row counts)
  3. DQ results → dq_results

Answers: "What happened during yesterday's pipeline run?"

Usage: python collect_observability.py
"""
import os
from datetime import datetime

import boto3
import psycopg2
import clickhouse_connect


PG_CONN = {
    "host": os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "").split("@")[-1].split("/")[0] if "@" in os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "") else "airflow-postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}
# Simplify: just use known values
PG_CONN = {"host": "airflow-postgres", "port": 5432, "database": "airflow",
           "user": "airflow", "password": "airflow"}

CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minio_access_key")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio_secret_key")


def collect_airflow_metrics(pg_conn, ch_client):
    """Pull DAG run and task metrics from Airflow's metadata DB."""
    cur = pg_conn.cursor()

    # ── Pipeline runs ──
    cur.execute("""
        SELECT
            run_id, dag_id,
            COALESCE(data_interval_end::date, execution_date::date) AS batch_date,
            state,
            start_date, end_date,
            EXTRACT(EPOCH FROM (end_date - start_date)) AS duration_seconds
        FROM dag_run
        ORDER BY start_date DESC
        LIMIT 50
    """)
    runs = cur.fetchall()

    run_data = []
    for run_id, dag_id, batch_date, state, started, ended, dur in runs:
        # Get task counts for this run
        cur.execute("""
            SELECT
                count(*) AS total,
                count(*) FILTER (WHERE state = 'success') AS succeeded,
                count(*) FILTER (WHERE state = 'failed') AS failed
            FROM task_instance
            WHERE dag_id = %s AND run_id = %s
        """, (dag_id, run_id))
        total, succeeded, failed = cur.fetchone()

        from datetime import date as date_type
        bd = batch_date if isinstance(batch_date, date_type) else datetime.strptime(str(batch_date), "%Y-%m-%d").date()
        run_data.append([
            run_id, dag_id, bd, state or "unknown",
            started or datetime(2000, 1, 1), ended,
            dur, total, succeeded, failed,
        ])

    if run_data:
        ch_client.insert("observability.pipeline_runs", run_data,
                         column_names=["run_id", "dag_id", "batch_date", "state",
                                       "started_at", "completed_at", "duration_seconds",
                                       "task_count", "tasks_succeeded", "tasks_failed"])

    # ── Task metrics ──
    cur.execute("""
        SELECT
            ti.run_id, ti.dag_id, ti.task_id,
            COALESCE(dr.data_interval_end::date, dr.execution_date::date) AS batch_date,
            ti.state,
            ti.start_date, ti.end_date,
            EXTRACT(EPOCH FROM (ti.end_date - ti.start_date)) AS duration_seconds,
            ti.try_number,
            COALESCE(ti.pool, 'default_pool') AS pool,
            COALESCE(ti.operator, 'unknown') AS operator
        FROM task_instance ti
        JOIN dag_run dr ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
        ORDER BY ti.start_date DESC NULLS LAST
        LIMIT 200
    """)
    tasks = cur.fetchall()

    task_data = []
    for row in tasks:
        run_id, dag_id, task_id, batch_date, state, started, ended, dur, try_num, pool, operator = row
        from datetime import date as date_type
        bd = batch_date if isinstance(batch_date, date_type) else datetime.strptime(str(batch_date), "%Y-%m-%d").date()
        task_data.append([
            run_id, dag_id, task_id, bd, state or "unknown",
            started, ended, dur, try_num, pool, operator,
        ])

    if task_data:
        ch_client.insert("observability.task_metrics", task_data,
                         column_names=["run_id", "dag_id", "task_id", "batch_date",
                                       "state", "started_at", "completed_at",
                                       "duration_seconds", "try_number", "pool", "operator"])

    cur.close()
    return len(run_data), len(task_data)


def collect_data_freshness(s3_client, ch_client):
    """Scan MinIO for latest partitions and sizes per layer/table."""
    bucket = "ecommerce-data"
    layers = {
        "bronze": ["orders", "order_items", "order_payments", "order_reviews",
                    "customers", "products", "sellers", "geolocation"],
        "silver": ["orders", "order_items", "order_payments", "order_reviews",
                    "customers", "products", "sellers", "geography"],
        "gold": ["dim_date", "dim_customer", "dim_product", "dim_seller",
                 "dim_geography", "dim_payment_type", "dim_customer_scd2",
                 "fact_order_items", "fact_orders", "fact_payments", "fact_reviews"],
    }

    freshness_data = []
    for layer, tables in layers.items():
        for table in tables:
            prefix = f"{layer}/{table}/"
            resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            contents = resp.get("Contents", [])

            if not contents:
                continue

            # Find latest partition and total size
            partitions = set()
            total_size = 0
            for obj in contents:
                key = obj["Key"]
                total_size += obj["Size"]
                # Extract partition from path
                parts = key.split("/")
                for p in parts:
                    if p.startswith("ingestion_date="):
                        partitions.add(p.replace("ingestion_date=", ""))

            latest_partition = max(partitions) if partitions else "none"

            freshness_data.append([
                layer, table, latest_partition, 0, total_size,
            ])

    if freshness_data:
        ch_client.insert("observability.data_freshness", freshness_data,
                         column_names=["layer", "table_name", "latest_partition",
                                       "row_count", "size_bytes"])

    return len(freshness_data)


def main():
    print(f"\n{'='*60}")
    print(f"  PIPELINE OBSERVABILITY COLLECTOR")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*60}\n")

    pg_conn = psycopg2.connect(**PG_CONN)
    ch_client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)
    s3_client = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                             aws_access_key_id=S3_KEY,
                             aws_secret_access_key=S3_SECRET)

    # ── Collect Airflow metrics ──
    print("  Collecting Airflow metrics...")
    runs, tasks = collect_airflow_metrics(pg_conn, ch_client)
    print(f"    Pipeline runs: {runs}")
    print(f"    Task metrics:  {tasks}")

    # ── Collect data freshness ──
    print("  Collecting data freshness...")
    freshness = collect_data_freshness(s3_client, ch_client)
    print(f"    Layer/table entries: {freshness}")

    pg_conn.close()

    # ── Print observability dashboard ──
    print(f"\n{'='*60}")
    print(f"  PIPELINE HEALTH DASHBOARD")
    print(f"{'='*60}")

    # Recent DAG runs
    print(f"\n  --- Recent DAG Runs ---")
    result = ch_client.query("""
        SELECT dag_id, batch_date, state,
               round(duration_seconds, 1) AS dur_sec,
               tasks_succeeded, tasks_failed
        FROM observability.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 15
    """)
    print(f"  {'DAG':<25} {'Date':<12} {'State':<10} {'Duration':>8} {'OK':>4} {'Fail':>4}")
    print(f"  {'-'*25} {'-'*12} {'-'*10} {'-'*8} {'-'*4} {'-'*4}")
    for row in result.result_rows:
        dag, dt, state, dur, ok, fail = row
        dur_str = f"{dur:.0f}s" if dur else "—"
        print(f"  {dag:<25} {str(dt):<12} {state:<10} {dur_str:>8} {ok:>4} {fail:>4}")

    # Slowest tasks
    print(f"\n  --- Slowest Tasks (recent) ---")
    result = ch_client.query("""
        SELECT dag_id, task_id, state,
               round(duration_seconds, 1) AS dur_sec,
               try_number
        FROM observability.task_metrics
        WHERE duration_seconds > 0
        ORDER BY duration_seconds DESC
        LIMIT 10
    """)
    print(f"  {'DAG':<25} {'Task':<30} {'State':<8} {'Duration':>8} {'Try':>4}")
    print(f"  {'-'*25} {'-'*30} {'-'*8} {'-'*8} {'-'*4}")
    for row in result.result_rows:
        dag, task, state, dur, try_num = row
        print(f"  {dag:<25} {task:<30} {state:<8} {dur:>7.1f}s {try_num:>4}")

    # Data freshness
    print(f"\n  --- Data Freshness ---")
    result = ch_client.query("""
        SELECT layer, table_name, latest_partition,
               formatReadableSize(size_bytes) AS size
        FROM observability.data_freshness
        ORDER BY layer, table_name
    """)
    current_layer = None
    for row in result.result_rows:
        layer, table, partition, size = row
        if layer != current_layer:
            current_layer = layer
            print(f"\n  [{layer.upper()}]")
        print(f"    {table:<25} latest={partition:<12} size={size}")

    # Failed tasks
    print(f"\n  --- Failed/Retried Tasks ---")
    result = ch_client.query("""
        SELECT dag_id, task_id, batch_date, try_number, state
        FROM observability.task_metrics
        WHERE state IN ('failed', 'up_for_retry') OR try_number > 1
        ORDER BY started_at DESC NULLS LAST
        LIMIT 10
    """)
    if result.result_rows:
        for row in result.result_rows:
            dag, task, dt, tries, state = row
            print(f"    {dag}.{task} [{dt}] state={state} tries={tries}")
    else:
        print(f"    No recent failures.")

    print(f"\n  Done.")


if __name__ == "__main__":
    main()
