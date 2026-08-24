-- ════════════════════════════════════════════════════════════
-- PIPELINE OBSERVABILITY QUERIES
-- ════════════════════════════════════════════════════════════

-- Q1: Pipeline run history (last 30 days)
SELECT
    dag_id,
    batch_date,
    state,
    round(duration_seconds, 0) AS dur_sec,
    tasks_succeeded AS ok,
    tasks_failed AS fail,
    started_at
FROM observability.pipeline_runs
ORDER BY started_at DESC
LIMIT 30;

-- Q2: Daily pipeline success rate
SELECT
    dag_id,
    countIf(state = 'success') AS succeeded,
    countIf(state = 'failed') AS failed,
    count() AS total,
    round(countIf(state = 'success') / count() * 100, 1) AS success_rate_pct
FROM observability.pipeline_runs
GROUP BY dag_id
ORDER BY dag_id;

-- Q3: Average task duration by task (identify bottlenecks)
SELECT
    dag_id,
    task_id,
    count() AS runs,
    round(avg(duration_seconds), 1) AS avg_dur_sec,
    round(max(duration_seconds), 1) AS max_dur_sec,
    round(min(duration_seconds), 1) AS min_dur_sec
FROM observability.task_metrics
WHERE state = 'success' AND duration_seconds > 0
GROUP BY dag_id, task_id
ORDER BY avg_dur_sec DESC
LIMIT 20;

-- Q4: Tasks with retries (reliability issues)
SELECT
    dag_id,
    task_id,
    max(try_number) AS max_tries,
    count() AS total_attempts,
    countIf(state = 'success') AS succeeded,
    countIf(state = 'failed') AS failed
FROM observability.task_metrics
WHERE try_number > 1
GROUP BY dag_id, task_id
ORDER BY max_tries DESC;

-- Q5: Data freshness lag
SELECT
    layer,
    table_name,
    latest_partition,
    dateDiff('day', toDate(latest_partition), today()) AS days_stale,
    formatReadableSize(size_bytes) AS size
FROM observability.data_freshness
WHERE latest_partition != 'none'
ORDER BY layer, days_stale DESC;

-- Q6: Data layer sizes
SELECT
    layer,
    count() AS tables,
    formatReadableSize(sum(size_bytes)) AS total_size
FROM observability.data_freshness
GROUP BY layer
ORDER BY sum(size_bytes) DESC;

-- Q7: Spark pool utilization (concurrent task analysis)
SELECT
    pool,
    dag_id,
    count() AS tasks,
    round(avg(duration_seconds), 1) AS avg_dur,
    round(sum(duration_seconds), 0) AS total_dur
FROM observability.task_metrics
WHERE pool != 'default_pool' AND state = 'success'
GROUP BY pool, dag_id
ORDER BY total_dur DESC;
