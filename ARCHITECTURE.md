# E-Commerce Batch Analytics Platform — Architecture

## System Overview

A production-style batch analytical data platform built around a real Brazilian
e-commerce dataset (~100K orders, 112K order items, 1M geolocation records),
extended with synthetic incremental data generation for demonstrating
operational pipeline engineering.

```
                    REAL DATASET (Olist)
                         |
                         v
                Synthetic Delta Generator
               /          |           \
              /           |            \
         new rows      updates      late arrivals
              \           |            /
               \          |           /
                         v
                  Source Batches (CSV)
                         |
                         v
              ┌──────────────────┐
              │  Apache Airflow  │  4 DAGs, 40+ tasks
              │  (Orchestration) │  pools, sensors, XCom
              └────────┬─────────┘
                       |
                       v
              ┌──────────────────┐
              │  Apache Spark    │  14 PySpark jobs
              │  (Transformation)│  client mode, 1 worker
              └────────┬─────────┘
                       |
              ┌────────┴─────────────────────────────┐
              |                                      |
              v                                      v
     ┌─────────────┐                        ┌──────────────┐
     │ MinIO (S3)  │                        │  ClickHouse  │
     │ Data Lake   │                        │  OLAP DW     │
     │ Bronze      │  ──── Gold ──────────> │  Star Schema │
     │ Silver      │                        │  Observ.     │
     │ Gold        │                        └──────────────┘
     └─────────────┘
```

## Technology Stack

| Component         | Technology              | Purpose                              |
|-------------------|-------------------------|--------------------------------------|
| Orchestration     | Apache Airflow 2.10     | DAG scheduling, retries, sensors     |
| Transformation    | Apache Spark 3.5 / PySpark | Distributed data processing       |
| Data Lake         | MinIO (S3-compatible)   | Layered Parquet storage              |
| OLAP Warehouse    | ClickHouse              | Column-oriented analytical queries   |
| Metadata DB       | PostgreSQL 15           | Airflow metadata + OLTP comparison   |
| Containerization  | Docker Compose          | 7-service reproducible environment   |

## Data Lake Architecture (MinIO)

```
s3://ecommerce-data/
├── bronze/                     Raw Parquet + ingestion metadata
│   ├── orders/ingestion_date=YYYY-MM-DD/
│   ├── order_items/...
│   ├── customers/...
│   └── ... (9 tables)
├── silver/                     Cleaned, standardized, deduplicated
│   ├── orders/ingestion_date=YYYY-MM-DD/
│   ├── geography/              (resolved from 1M → 19K rows)
│   └── ... (9 tables)
├── silver_rejected/            Quarantined invalid records
├── gold/                       Star-schema dimensional model
│   ├── dim_date/               1,096 rows
│   ├── dim_customer/           99,441 rows
│   ├── dim_customer_scd2/      182,624 rows (SCD Type 2)
│   ├── dim_product/            32,951 rows
│   ├── dim_seller/             3,095 rows
│   ├── dim_geography/          19,015 rows
│   ├── dim_payment_type/       5 rows
│   ├── fact_order_items/       114,836 rows
│   ├── fact_orders/            101,016 rows
│   ├── fact_payments/          105,461 rows
│   └── fact_reviews/           99,591 rows
└── metadata/                   Batch tracking, validation reports
```

**Storage sizes:** Bronze 54 MB → Silver 153 MB → Gold 43 MB
**Total warehouse (ClickHouse):** 55 MB compressed (vs 116 MB in PostgreSQL = 2.1x)

## Airflow DAGs

### 1. bronze_ingestion (13 tasks)
```
check_source_files → generate_batch_metadata → [ingest.* × 9] → validate_bronze → publish_batch
```
- TaskGroups, XCom, pools (spark_pool=2 slots)
- 6-check validation per table (PK, nulls, schema, reconciliation)
- Batch metadata written to MinIO

### 2. silver_transformation (11 tasks)
```
wait_for_bronze → [independent × 5] → [dependent × 3] + resolve_geography → check_refs
```
- ExternalTaskSensor waits for bronze_ingestion
- Two-wave FK-aware dependency structure
- 6 referential integrity checks

### 3. gold_warehouse (12 tasks)
```
wait_for_silver → [dimensions × 6] → [facts × 4] → verify_star_schema
```
- Builds star schema from Silver data
- Grain verification (zero violations across all fact tables)

### 4. incremental_pipeline (11 tasks)
```
generate → ingest → dq_bronze → merge → dq_silver → rebuild_gold → dq_gold → load_clickhouse
```
- depends_on_past=True for day chaining
- DQ gates at each layer transition
- UPSERT merge logic for Silver
- Full Gold + ClickHouse rebuild

## Spark Jobs (14 PySpark scripts, ~5,200 lines)

| Job                        | Purpose                                        |
|----------------------------|------------------------------------------------|
| ingest_to_bronze.py        | CSV → Bronze Parquet with metadata              |
| ingest_incremental.py      | Incremental CSV → Bronze (new + late arrivals)  |
| bronze_to_silver.py        | Clean, standardize, deduplicate, validate       |
| resolve_geography.py       | 1M geolocation → 19K canonical ZIP records      |
| merge_silver.py            | PK-based UPSERT merge for incremental processing|
| build_dimensions.py        | 6 dimension tables with surrogate keys          |
| build_facts.py             | 4 fact tables with SK lookups                   |
| build_scd2_customer.py     | SCD Type 2 with segment + city changes          |
| build_facts_scd2.py        | Temporal fact join (range-based SK lookup)       |
| data_quality.py            | 101 automated checks across 3 layers            |
| generate_incremental.py    | Synthetic daily deltas preserving distributions  |
| benchmark_csv_vs_parquet.py| Storage and query performance comparison         |
| validate_bronze.py         | Bronze-layer data validation                    |
| collect_observability.py   | Pipeline metrics → ClickHouse observability      |

## Dimensional Model

### Star Schema
```
              dim_date ──────┐
                             │
dim_customer ──── fact_order_items ──── dim_product
                             │
              dim_seller ────┘
```

### Fact Table Grains
| Fact              | Grain                          | Key Measures                    |
|-------------------|--------------------------------|---------------------------------|
| fact_order_items  | One row per order item         | price, freight_value (additive) |
| fact_orders       | One row per order              | order_value, delivery_days      |
| fact_payments     | One row per payment            | payment_value, installments     |
| fact_reviews      | One row per review             | review_score (semi-additive)    |

### SCD Type 2 (dim_customer_scd2)
- 182,624 version rows for 98,666 unique customers
- 75% of customers have multiple versions
- Tracks: customer_segment (Bronze/Silver/Gold/Platinum) + city changes
- Fields: effective_from, effective_to, is_current
- Integrity: zero overlaps, exactly 1 current per customer

## Data Quality Framework (101 checks)

| Layer   | Checks | Categories                                          |
|---------|--------|-----------------------------------------------------|
| Bronze  | 28     | Existence, non-empty, schema, metadata              |
| Silver  | 27     | PK uniqueness, valid ranges, FKs, reconciliation    |
| Gold    | 46     | SK integrity, grain, SCD2, measure validation       |

Critical failures exit code 1 → Airflow task fails → downstream blocked.

## Incremental Processing

**Daily batch flow:**
- 500 new orders + 25 late-arriving orders + 50 customer updates
- UPSERT merge: left_anti join + union + dedup by PK
- Idempotent: re-running same batch → identical output
- 3 days processed: 99,441 → 99,966 → 100,491 → 101,016 orders

## Performance Benchmarks

### CSV vs Parquet (Phase 3)
| Table        | CSV    | Parquet | Compression |
|--------------|--------|---------|-------------|
| geolocation  | 58 MB  | 16 MB   | 3.6x        |
| orders       | 17 MB  | 10 MB   | 1.6x        |

Column pruning: 2.0x faster. Predicate pushdown: 2.3x faster (geolocation).

### OLTP vs OLAP (Phase 34)
| Query                              | PostgreSQL | ClickHouse | Speedup |
|------------------------------------|------------|------------|---------|
| Monthly Revenue (JOIN+GROUP BY)    | 126 ms     | 13 ms      | 9.8x   |
| Revenue by State (3-table join)    | 124 ms     | 17 ms      | 7.5x   |
| Revenue by Category                | 22 ms      | 10 ms      | 2.3x   |
| Storage                            | 116 MB     | 55 MB      | 2.1x   |

## Pipeline Observability

**ClickHouse `observability` database:**
- pipeline_runs: DAG run tracking (state, duration, task counts)
- task_metrics: Per-task execution details (duration, retries, pool)
- data_freshness: Latest partition dates and sizes per layer/table
- 7 operational monitoring queries

## Technologies Deliberately Excluded

| Technology | Reason                                            |
|------------|---------------------------------------------------|
| Kafka      | Batch project — streaming is a separate concern   |
| Flink      | No real-time processing requirement               |
| Kubernetes | Docker Compose sufficient for local development   |
| Redis      | No caching requirement                            |
| ML models  | Outside analytical data engineering scope         |
| dbt        | Spark handles transformations directly            |

## Key Engineering Questions Answered

1. **Why not query PostgreSQL directly?** → 9.8x slower, 2.1x more storage
2. **Why star schema?** → Optimized for analytical aggregations, not transactions
3. **Why surrogate keys?** → Enable SCD Type 2 temporal lookups
4. **Why Parquet?** → 1.6-3.6x compression, predicate pushdown, column pruning
5. **Why Bronze/Silver/Gold?** → Auditability, replay, separation of concerns
6. **How is idempotency achieved?** → Partition overwrite + PK-deterministic merge
7. **How are late arrivals handled?** → Ingested into current batch, merged by PK
8. **How does Airflow recover from failure?** → Retries, depends_on_past, catchup
9. **How is data quality enforced?** → 101 automated checks gating each layer
10. **How is freshness monitored?** → Observability collector → ClickHouse queries
