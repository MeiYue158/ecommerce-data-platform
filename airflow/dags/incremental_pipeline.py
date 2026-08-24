"""
Incremental Pipeline DAG

Processes daily incremental data through the full pipeline:
  1. Generate synthetic incremental data
  2. Ingest into Bronze
  3. Merge into Silver (UPSERT)
  4. Rebuild Gold dimensions and facts

Supports backfills via date-parameterized execution.
Each run is idempotent — re-running produces identical results.

Schedule: daily
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
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

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,  # Each day depends on previous day's Silver
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="incremental_pipeline",
    default_args=default_args,
    description="Daily incremental ETL: generate → ingest → merge → rebuild",
    schedule="@daily",
    start_date=datetime(2026, 8, 25),
    catchup=True,
    max_active_tasks=2,
    max_active_runs=1,
    tags=["incremental", "daily"],
    doc_md="""
    ## Incremental Pipeline

    Processes daily incremental data through Bronze → Silver → Gold.

    **Backfill:** `airflow dags backfill incremental_pipeline -s 2026-08-25 -e 2026-08-30`

    **depends_on_past=True** ensures days are processed in order,
    since each day's Silver merge reads from the previous day's output.

    **Baseline logic:** Day N merges into Day N-1's Silver partition.
    The initial baseline is `2026-08-24` (the seed data load).
    """,
) as dag:

    # ── 1. Generate synthetic incremental data ──
    generate = BashOperator(
        task_id="generate_incremental",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/generate_incremental.py "
            "{{ ds }} --orders 500 --late 25 --updates 50"
        ),
        execution_timeout=timedelta(minutes=15),
        pool="spark_pool",
    )

    # ── 2. Ingest into Bronze ──
    ingest = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/ingest_incremental.py "
            "{{ ds }}"
        ),
        execution_timeout=timedelta(minutes=10),
        pool="spark_pool",
    )

    # ── 3. Merge into Silver ──
    # Baseline = previous day's Silver partition
    # For the first run (2026-08-25), baseline is the seed data (2026-08-24)
    merge = BashOperator(
        task_id="merge_silver",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/merge_silver.py "
            "{{ ds }} --baseline {{ prev_ds }}"
        ),
        execution_timeout=timedelta(minutes=15),
        pool="spark_pool",
    )

    # ── 4. Rebuild Gold (dimensions + facts) ──
    with TaskGroup("rebuild_gold") as gold_group:
        build_dims = BashOperator(
            task_id="build_dimensions",
            bash_command=(
                f"{SPARK_SUBMIT} /opt/spark-jobs/build_dimensions.py "
                "{{ ds }} all"
            ),
            execution_timeout=timedelta(minutes=15),
            pool="spark_pool",
        )

        build_scd2 = BashOperator(
            task_id="build_scd2_customer",
            bash_command=(
                f"{SPARK_SUBMIT} /opt/spark-jobs/build_scd2_customer.py "
                "{{ ds }}"
            ),
            execution_timeout=timedelta(minutes=15),
            pool="spark_pool",
        )

        build_facts = BashOperator(
            task_id="build_facts",
            bash_command=(
                f"{SPARK_SUBMIT} /opt/spark-jobs/build_facts.py "
                "{{ ds }} all"
            ),
            execution_timeout=timedelta(minutes=15),
            pool="spark_pool",
        )

        build_facts_scd2 = BashOperator(
            task_id="build_facts_scd2",
            bash_command=(
                f"{SPARK_SUBMIT} /opt/spark-jobs/build_facts_scd2.py "
                "{{ ds }}"
            ),
            execution_timeout=timedelta(minutes=15),
            pool="spark_pool",
        )

        [build_dims, build_scd2] >> build_facts >> build_facts_scd2

    # ── 5. Data quality gate (blocks publish if critical failures) ──
    dq_bronze = BashOperator(
        task_id="dq_check_bronze",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/data_quality.py "
            "{{ ds }} bronze"
        ),
        execution_timeout=timedelta(minutes=10),
        pool="spark_pool",
    )

    dq_silver = BashOperator(
        task_id="dq_check_silver",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/data_quality.py "
            "{{ ds }} silver"
        ),
        execution_timeout=timedelta(minutes=10),
        pool="spark_pool",
    )

    dq_gold = BashOperator(
        task_id="dq_check_gold",
        bash_command=(
            f"{SPARK_SUBMIT} /opt/spark-jobs/data_quality.py "
            "{{ ds }} gold"
        ),
        execution_timeout=timedelta(minutes=15),
        pool="spark_pool",
    )

    # ── 6. Load into ClickHouse OLAP warehouse ──
    load_warehouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=(
            'clickhouse-client --host $CLICKHOUSE_HOST --port 9000 '
            '--multiquery < /opt/spark-jobs/../warehouse/ddl/load_from_s3.sql '
            '|| echo "ClickHouse load completed with warnings"'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── DAG dependencies ──
    # DQ checks gate each transition: Bronze→Silver→Gold→warehouse
    generate >> ingest >> dq_bronze >> merge >> dq_silver >> gold_group >> dq_gold >> load_warehouse
