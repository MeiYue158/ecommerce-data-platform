"""
Gold Warehouse DAG

Builds the star-schema dimensional model from Silver data.
Dimensions must complete before facts (facts join dimensions for SK resolution).

DAG structure:
    wait_for_silver
        |
        v
    [build_dimensions]  — dim_date, dim_customer, dim_product, dim_seller,
        |                  dim_geography, dim_payment_type
        v
    [build_facts]       — fact_order_items, fact_orders, fact_payments, fact_reviews
        |
        v
    verify_star_schema
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

DIMENSIONS = [
    "dim_date", "dim_customer", "dim_product",
    "dim_seller", "dim_geography", "dim_payment_type",
]

FACTS = [
    "fact_order_items", "fact_orders",
    "fact_payments", "fact_reviews",
]

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="gold_warehouse",
    default_args=default_args,
    description="Build star-schema dimensional model (Gold layer)",
    schedule="@daily",
    start_date=datetime(2026, 8, 24),
    catchup=True,
    max_active_tasks=3,
    max_active_runs=1,
    tags=["gold", "warehouse", "dimensional-model"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_transformation",
        external_task_id="check_referential_integrity",
        mode="reschedule",
        timeout=3600,
        poke_interval=30,
    )

    with TaskGroup("build_dimensions") as dim_group:
        for dim in DIMENSIONS:
            BashOperator(
                task_id=dim,
                bash_command=(
                    f"{SPARK_SUBMIT} /opt/spark-jobs/build_dimensions.py "
                    f"{{{{ ds }}}} {dim}"
                ),
                execution_timeout=timedelta(minutes=15),
                pool="spark_pool",
            )

    with TaskGroup("build_facts") as fact_group:
        for fact in FACTS:
            BashOperator(
                task_id=fact,
                bash_command=(
                    f"{SPARK_SUBMIT} /opt/spark-jobs/build_facts.py "
                    f"{{{{ ds }}}} {fact}"
                ),
                execution_timeout=timedelta(minutes=15),
                pool="spark_pool",
            )

    verify = BashOperator(
        task_id="verify_star_schema",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/verify_star_schema.py "
            "2>&1 | grep -E '---|PASS|WARN|grain|revenue'"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    wait_for_silver >> dim_group >> fact_group >> verify
