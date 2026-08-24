"""
Bronze Layer Validation

Runs data quality checks on the Bronze layer after ingestion:
  1. Row counts (non-empty)
  2. Primary key uniqueness
  3. Null primary key detection
  4. Expected columns present
  5. Source-to-target row count reconciliation

Exit code 1 if any critical check fails.

Usage:
    spark-submit validate_bronze.py <ingestion_date>
"""
import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_BASE = "s3a://ecommerce-data/bronze"

# Table definitions: (table_name, primary_key_columns, expected_columns)
TABLE_CHECKS = {
    "orders": {
        "pk": ["order_id"],
        "required_cols": [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp",
        ],
        "source_csv": "olist_orders_dataset.csv",
    },
    "order_items": {
        "pk": ["order_id", "order_item_id"],
        "required_cols": [
            "order_id", "order_item_id", "product_id",
            "seller_id", "price",
        ],
        "source_csv": "olist_order_items_dataset.csv",
    },
    "order_payments": {
        "pk": ["order_id", "payment_sequential"],
        "required_cols": [
            "order_id", "payment_sequential", "payment_type",
            "payment_value",
        ],
        "source_csv": "olist_order_payments_dataset.csv",
    },
    "order_reviews": {
        "pk": ["review_id"],
        "required_cols": ["review_id", "order_id", "review_score"],
        "source_csv": "olist_order_reviews_dataset.csv",
    },
    "customers": {
        "pk": ["customer_id"],
        "required_cols": [
            "customer_id", "customer_unique_id", "customer_city",
            "customer_state",
        ],
        "source_csv": "olist_customers_dataset.csv",
    },
    "products": {
        "pk": ["product_id"],
        "required_cols": ["product_id", "product_category_name"],
        "source_csv": "olist_products_dataset.csv",
    },
    "sellers": {
        "pk": ["seller_id"],
        "required_cols": [
            "seller_id", "seller_city", "seller_state",
        ],
        "source_csv": "olist_sellers_dataset.csv",
    },
    "geolocation": {
        "pk": [],  # No unique PK — multiple coords per zip
        "required_cols": [
            "geolocation_zip_code_prefix", "geolocation_lat",
            "geolocation_lng", "geolocation_state",
        ],
        "source_csv": "olist_geolocation_dataset.csv",
    },
    "category_translation": {
        "pk": ["product_category_name"],
        "required_cols": [
            "product_category_name",
            "product_category_name_english",
        ],
        "source_csv": "product_category_name_translation.csv",
    },
}

SOURCE_DIR = "/opt/data/seed"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("validate_bronze")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def validate_table(spark, table_name, checks, ingestion_date):
    """Run all checks for a single table. Returns dict of results."""
    path = f"{BRONZE_BASE}/{table_name}/ingestion_date={ingestion_date}"
    result = {
        "table": table_name,
        "path": path,
        "checks": [],
        "passed": True,
    }

    # Read Bronze data
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        result["checks"].append({
            "check": "table_exists",
            "status": "FAIL",
            "detail": str(e),
        })
        result["passed"] = False
        return result

    data_cols = [c for c in df.columns if not c.startswith("_")]
    row_count = df.count()

    # ── Check 1: Non-empty ──
    status = "PASS" if row_count > 0 else "FAIL"
    result["checks"].append({
        "check": "row_count > 0",
        "status": status,
        "detail": f"{row_count:,} rows",
    })
    result["row_count"] = row_count
    if status == "FAIL":
        result["passed"] = False

    # ── Check 2: Required columns present ──
    missing = [c for c in checks["required_cols"] if c not in data_cols]
    status = "PASS" if not missing else "FAIL"
    result["checks"].append({
        "check": "required_columns",
        "status": status,
        "detail": f"missing: {missing}" if missing else "all present",
    })
    if status == "FAIL":
        result["passed"] = False

    # ── Check 3: Primary key uniqueness ──
    pk_cols = checks["pk"]
    if pk_cols:
        distinct_count = df.select(pk_cols).distinct().count()
        duplicates = row_count - distinct_count
        status = "PASS" if duplicates == 0 else "WARN"
        result["checks"].append({
            "check": "pk_uniqueness",
            "status": status,
            "detail": f"{duplicates:,} duplicate keys" if duplicates else "unique",
        })
        if status == "WARN":
            result["has_warnings"] = True
    else:
        result["checks"].append({
            "check": "pk_uniqueness",
            "status": "SKIP",
            "detail": "no PK defined",
        })

    # ── Check 4: Null primary keys ──
    if pk_cols:
        null_filter = None
        for col in pk_cols:
            cond = F.col(col).isNull()
            null_filter = cond if null_filter is None else (null_filter | cond)
        null_pk_count = df.filter(null_filter).count()
        status = "PASS" if null_pk_count == 0 else "FAIL"
        result["checks"].append({
            "check": "pk_not_null",
            "status": status,
            "detail": f"{null_pk_count:,} null PKs" if null_pk_count else "no nulls",
        })
        if status == "FAIL":
            result["passed"] = False

    # ── Check 5: Metadata columns present ──
    meta_cols = ["_ingested_at", "_source_file", "_batch_id", "_source_system"]
    missing_meta = [c for c in meta_cols if c not in df.columns]
    status = "PASS" if not missing_meta else "FAIL"
    result["checks"].append({
        "check": "metadata_columns",
        "status": status,
        "detail": f"missing: {missing_meta}" if missing_meta else "all present",
    })
    if status == "FAIL":
        result["passed"] = False

    # ── Check 6: Source-to-target reconciliation ──
    source_path = f"{SOURCE_DIR}/{checks['source_csv']}"
    try:
        source_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(source_path)
        )
        source_count = source_df.count()
        diff = abs(row_count - source_count)
        status = "PASS" if diff == 0 else "WARN"
        result["checks"].append({
            "check": "source_target_reconciliation",
            "status": status,
            "detail": f"source={source_count:,} target={row_count:,} diff={diff:,}",
        })
        result["source_count"] = source_count
        if status == "WARN":
            result["has_warnings"] = True
    except Exception:
        result["checks"].append({
            "check": "source_target_reconciliation",
            "status": "SKIP",
            "detail": "source file not available",
        })

    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: validate_bronze.py <ingestion_date>")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*70}")
    print(f"  BRONZE LAYER VALIDATION — {ingestion_date}")
    print(f"{'='*70}\n")

    all_results = []
    failures = 0
    warnings = 0

    for table_name, checks in TABLE_CHECKS.items():
        result = validate_table(spark, table_name, checks, ingestion_date)
        all_results.append(result)

        status_icon = "PASS" if result["passed"] else "FAIL"
        if result.get("has_warnings"):
            warnings += 1
        if not result["passed"]:
            failures += 1

        row_info = f"{result.get('row_count', 0):>10,} rows"
        print(f"  [{status_icon}] {table_name:<25} {row_info}")
        for check in result["checks"]:
            marker = {"PASS": " ", "FAIL": "X", "WARN": "!", "SKIP": "-"}
            print(f"        [{marker.get(check['status'], '?')}] {check['check']}: {check['detail']}")

    # ── Summary ──
    total = len(all_results)
    passed = total - failures
    print(f"\n{'='*70}")
    print(f"  SUMMARY: {passed}/{total} tables passed, {warnings} warnings, {failures} failures")
    print(f"{'='*70}\n")

    # Output structured validation report (captured by Airflow task logs)
    report = {
        "ingestion_date": ingestion_date,
        "validated_at": datetime.utcnow().isoformat(),
        "total_tables": total,
        "passed": passed,
        "warnings": warnings,
        "failures": failures,
        "row_counts": {
            r["table"]: r.get("row_count", 0) for r in all_results
        },
    }
    print(f"  VALIDATION_REPORT_JSON={json.dumps(report)}")

    spark.stop()

    if failures > 0:
        print(f"\nFAILED: {failures} table(s) did not pass validation")
        sys.exit(1)


if __name__ == "__main__":
    main()
