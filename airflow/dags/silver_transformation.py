"""
Silver Transformation DAG

Transforms Bronze data into cleaned, standardized, deduplicated Silver datasets.
Runs after bronze_ingestion completes.

DAG structure:
    check_bronze
        |
        +---> [independent tables: orders, customers, products, sellers, category_translation]
        |         |
        |         v
        +---> [dependent tables: order_items, order_payments, order_reviews]
        |         |
        |         v
        +---> check_referential_integrity
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup


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

TRANSFORM_SCRIPT = "/opt/spark-jobs/bronze_to_silver.py"

# Tables with no FK dependencies — can run first
INDEPENDENT_TABLES = ["orders", "customers", "products", "sellers", "category_translation"]

# Tables that depend on independent tables being in Silver
DEPENDENT_TABLES = ["order_items", "order_payments", "order_reviews"]

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="silver_transformation",
    default_args=default_args,
    description="Transform Bronze → Silver (clean, standardize, deduplicate)",
    schedule="@daily",
    start_date=datetime(2026, 8, 24),
    catchup=True,
    max_active_tasks=3,
    max_active_runs=1,
    tags=["silver", "transformation"],
    doc_md="""
    ## Silver Transformation Pipeline

    Transforms Bronze Parquet data into clean, standardized Silver datasets.

    **Transformations applied:**
    - Schema standardization (types, formats)
    - City name normalization (initcap)
    - State code standardization (uppercase)
    - ZIP code padding (5 digits)
    - Product category English translation
    - Review deduplication (keep latest by review_id)
    - Business rule validation (non-negative prices, valid statuses)
    - Rejected records quarantined to silver_rejected/

    **Depends on:** `bronze_ingestion` DAG
    """,
) as dag:

    # ── Wait for Bronze ingestion to complete ──
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_ingestion",
        external_task_id="publish_batch",
        mode="reschedule",
        timeout=3600,
        poke_interval=30,
    )

    # ── Independent tables (no FK dependencies) ──
    with TaskGroup("transform_independent", tooltip="Tables with no FK deps") as independent_group:
        independent_tasks = {}
        for table in INDEPENDENT_TABLES:
            task = BashOperator(
                task_id=table,
                bash_command=(
                    f"{SPARK_SUBMIT} {TRANSFORM_SCRIPT} "
                    f"{{{{ ds }}}} {table}"
                ),
                execution_timeout=timedelta(minutes=15),
                pool="spark_pool",
            )
            independent_tasks[table] = task

    # ── Dependent tables (need FK sources in Silver first) ──
    with TaskGroup("transform_dependent", tooltip="Tables with FK deps") as dependent_group:
        for table in DEPENDENT_TABLES:
            BashOperator(
                task_id=table,
                bash_command=(
                    f"{SPARK_SUBMIT} {TRANSFORM_SCRIPT} "
                    f"{{{{ ds }}}} {table}"
                ),
                execution_timeout=timedelta(minutes=15),
                pool="spark_pool",
            )

    # ── Resolve geolocation (1M rows → ~19K canonical ZIP records) ──
    resolve_geo = BashOperator(
        task_id="resolve_geography",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/resolve_geography.py "
            "{{ ds }}"
        ),
        execution_timeout=timedelta(minutes=15),
        pool="spark_pool",
    )

    # ── Referential integrity check ──
    check_refs = BashOperator(
        task_id="check_referential_integrity",
        bash_command=(
            f"{SPARK_SUBMIT} {TRANSFORM_SCRIPT} "
            "{{ ds }} check_refs"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── DAG dependencies ──
    # Geography runs in parallel with dependent tables (no FK dependency)
    wait_for_bronze >> independent_group >> [dependent_group, resolve_geo] >> check_refs
