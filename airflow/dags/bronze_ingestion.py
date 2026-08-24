"""
Bronze Ingestion DAG

Orchestrates loading of source CSV files into the MinIO Bronze layer as Parquet.
Demonstrates: TaskGroups, XCom, pools, batch metadata, parameterized execution,
retries, catchup/backfill support, and pipeline observability.

DAG structure:
    check_sources -> generate_batch -> [ingest group] -> validate_bronze -> publish_batch
"""
import json
import uuid
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────

SPARK_SUBMIT = (
    "spark-submit "
    "--master $SPARK_MASTER_URL "
    "--conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT "
    "--conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID "
    "--conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.driver.host=$(hostname -i) "
)

INGEST_SCRIPT = "/opt/spark-jobs/ingest_to_bronze.py"
VALIDATE_SCRIPT = "/opt/spark-jobs/validate_bronze.py"

MINIO_BUCKET = "ecommerce-data"
MINIO_ENDPOINT = "http://minio:9000"

SOURCE_TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
]


# ──────────────────────────────────────────────────────────────
# Python callables
# ──────────────────────────────────────────────────────────────

def _get_s3_client():
    import os
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def generate_batch_metadata(**context):
    """Generate a unique batch ID and write initial metadata to MinIO."""
    ds = context["ds"]
    batch_id = f"batch_{ds}_{uuid.uuid4().hex[:8]}"

    metadata = {
        "batch_id": batch_id,
        "ingestion_date": ds,
        "dag_run_id": context["run_id"],
        "started_at": datetime.utcnow().isoformat(),
        "status": "running",
        "tables": {t: {"status": "pending"} for t in SOURCE_TABLES},
    }

    s3 = _get_s3_client()
    key = f"metadata/bronze/batches/{ds}/{batch_id}.json"
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(metadata, indent=2),
        ContentType="application/json",
    )

    # Push batch_id to XCom so downstream tasks can use it
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="metadata_key", value=key)
    print(f"Batch {batch_id} initialized -> s3://{MINIO_BUCKET}/{key}")
    return batch_id


def publish_batch(**context):
    """Mark batch as completed with final row counts from validation."""
    ti = context["ti"]
    ds = context["ds"]
    batch_id = ti.xcom_pull(task_ids="generate_batch_metadata", key="batch_id")
    metadata_key = ti.xcom_pull(task_ids="generate_batch_metadata", key="metadata_key")
    validation_result = ti.xcom_pull(task_ids="validate_bronze", key="return_value")

    s3 = _get_s3_client()

    # Read existing metadata
    obj = s3.get_object(Bucket=MINIO_BUCKET, Key=metadata_key)
    metadata = json.loads(obj["Body"].read().decode())

    # Update with completion info
    metadata["status"] = "completed"
    metadata["completed_at"] = datetime.utcnow().isoformat()

    # Parse validation output for row counts if available
    if validation_result:
        metadata["validation_output"] = validation_result

    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2),
        ContentType="application/json",
    )

    print(f"Batch {batch_id} published as COMPLETED")
    print(f"  Metadata: s3://{MINIO_BUCKET}/{metadata_key}")
    return metadata


def on_failure_callback(context):
    """Log failure details for observability."""
    ti = context["task_instance"]
    print(f"TASK FAILED: {ti.dag_id}.{ti.task_id}")
    print(f"  Run ID:    {context['run_id']}")
    print(f"  Try:       {ti.try_number}")
    print(f"  Exception: {context.get('exception', 'unknown')}")


# ──────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="bronze_ingestion",
    default_args=default_args,
    description="Ingest source CSVs into Bronze layer (MinIO/Parquet)",
    schedule="@daily",
    start_date=datetime(2026, 8, 24),
    catchup=True,
    max_active_tasks=3,
    max_active_runs=1,
    tags=["bronze", "ingestion"],
    params={
        "tables": SOURCE_TABLES,
    },
    doc_md="""
    ## Bronze Ingestion Pipeline

    Loads source CSV files into the Bronze layer in MinIO as Parquet files
    with ingestion metadata (`_ingested_at`, `_source_file`, `_batch_id`, `_source_system`).

    **Backfill:** `airflow dags backfill bronze_ingestion -s 2026-08-20 -e 2026-08-24`

    **Rerun single date:** Clear tasks for the target date and let scheduler retry.

    **Idempotency:** Each run overwrites the partition for its ingestion_date,
    so reruns produce identical results without duplicates.
    """,
) as dag:

    # ── 1. Check source files exist ──
    check_sources = BashOperator(
        task_id="check_source_files",
        bash_command=(
            'echo "Checking source files for {{ ds }}..." && '
            "ls /opt/data/seed/olist_orders_dataset.csv "
            "/opt/data/seed/olist_customers_dataset.csv "
            "/opt/data/seed/olist_products_dataset.csv "
            "/opt/data/seed/olist_sellers_dataset.csv "
            "/opt/data/seed/olist_order_items_dataset.csv "
            "> /dev/null && "
            'echo "All source files present"'
        ),
    )

    # ── 2. Generate batch metadata ──
    generate_batch = PythonOperator(
        task_id="generate_batch_metadata",
        python_callable=generate_batch_metadata,
    )

    # ── 3. Ingest tables (TaskGroup for visual organization) ──
    with TaskGroup("ingest", tooltip="Parallel ingestion of source tables") as ingest_group:
        for table in SOURCE_TABLES:
            BashOperator(
                task_id=table,
                bash_command=(
                    f"{SPARK_SUBMIT} {INGEST_SCRIPT} "
                    f"{{{{ ds }}}} {table}"
                ),
                execution_timeout=timedelta(minutes=20),
                pool="spark_pool",
            )

    # ── 4. Validate Bronze data ──
    validate = BashOperator(
        task_id="validate_bronze",
        bash_command=(
            f"{SPARK_SUBMIT} {VALIDATE_SCRIPT} "
            "{{ ds }}"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # ── 5. Publish batch completion ──
    publish = PythonOperator(
        task_id="publish_batch",
        python_callable=publish_batch,
    )

    # ── DAG dependencies ──
    check_sources >> generate_batch >> ingest_group >> validate >> publish
