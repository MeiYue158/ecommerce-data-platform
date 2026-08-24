-- Pipeline Observability Tables in ClickHouse

CREATE DATABASE IF NOT EXISTS observability;

-- One row per DAG run
CREATE TABLE IF NOT EXISTS observability.pipeline_runs (
    run_id              String,
    dag_id              String,
    batch_date          Date,
    state               String,
    started_at          DateTime,
    completed_at        Nullable(DateTime),
    duration_seconds    Nullable(Float64),
    task_count          Int32,
    tasks_succeeded     Int32,
    tasks_failed        Int32,
    collected_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(collected_at)
ORDER BY (dag_id, run_id);

-- One row per task execution
CREATE TABLE IF NOT EXISTS observability.task_metrics (
    run_id              String,
    dag_id              String,
    task_id             String,
    batch_date          Date,
    state               String,
    started_at          Nullable(DateTime),
    completed_at        Nullable(DateTime),
    duration_seconds    Nullable(Float64),
    try_number          Int32,
    pool                String,
    operator            String,
    collected_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(collected_at)
ORDER BY (dag_id, run_id, task_id);

-- Data layer freshness and sizes
CREATE TABLE IF NOT EXISTS observability.data_freshness (
    layer               String,
    table_name          String,
    latest_partition    String,
    row_count           Int64,
    size_bytes          Int64,
    checked_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (layer, table_name);

-- DQ check results per run
CREATE TABLE IF NOT EXISTS observability.dq_results (
    batch_date          Date,
    layer               String,
    total_checks        Int32,
    passed              Int32,
    warnings            Int32,
    critical_failures   Int32,
    checked_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (batch_date, layer);
