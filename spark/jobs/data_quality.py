"""
Data Quality Framework

Runs automated quality checks at each pipeline layer:
  - Bronze: existence, non-empty, schema recognition
  - Silver: PK uniqueness, data types, valid ranges, business rules
  - Referential: FK integrity across Silver tables
  - Gold/Warehouse: fact SK resolution, SCD integrity, grain uniqueness

Exit code 1 if any CRITICAL check fails (blocks downstream).
Warnings are reported but do not block.

Usage:
    spark-submit data_quality.py <ingestion_date> <layer>
    spark-submit data_quality.py 2026-08-24 bronze
    spark-submit data_quality.py 2026-08-24 silver
    spark-submit data_quality.py 2026-08-24 gold
    spark-submit data_quality.py 2026-08-24 all
"""
import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_BASE = "s3a://ecommerce-data/bronze"
SILVER_BASE = "s3a://ecommerce-data/silver"
GOLD_BASE = "s3a://ecommerce-data/gold"

VALID_ORDER_STATUSES = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
]
VALID_PAYMENT_TYPES = [
    "credit_card", "boleto", "voucher", "debit_card", "not_defined",
]
VALID_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
    "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
    "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("data_quality")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


class DQReport:
    """Collects and displays data quality check results."""

    def __init__(self, layer, ingestion_date):
        self.layer = layer
        self.ingestion_date = ingestion_date
        self.checks = []
        self.critical_failures = 0
        self.warnings = 0
        self.passes = 0

    def add(self, table, check_name, passed, severity="CRITICAL", detail="", value=None):
        status = "PASS" if passed else severity
        self.checks.append({
            "table": table,
            "check": check_name,
            "status": status,
            "detail": detail,
            "value": value,
        })
        if passed:
            self.passes += 1
        elif severity == "CRITICAL":
            self.critical_failures += 1
        else:
            self.warnings += 1

    def print_report(self):
        print(f"\n  {'='*70}")
        print(f"  DATA QUALITY REPORT — {self.layer.upper()} — {self.ingestion_date}")
        print(f"  {'='*70}\n")

        current_table = None
        for c in self.checks:
            if c["table"] != current_table:
                current_table = c["table"]
                print(f"  [{current_table}]")
            icon = {
                "PASS": " ok ",
                "CRITICAL": "FAIL",
                "WARN": "warn",
            }.get(c["status"], "????")
            detail = f" — {c['detail']}" if c["detail"] else ""
            print(f"    [{icon}] {c['check']}{detail}")

        total = self.passes + self.critical_failures + self.warnings
        print(f"\n  {'─'*70}")
        print(f"  TOTAL: {total} checks | "
              f"{self.passes} passed | "
              f"{self.warnings} warnings | "
              f"{self.critical_failures} CRITICAL failures")
        print(f"  {'─'*70}")

        if self.critical_failures > 0:
            print(f"\n  *** PIPELINE BLOCKED: {self.critical_failures} critical failure(s) ***")
        return self.critical_failures == 0

    def to_json(self):
        return {
            "layer": self.layer,
            "ingestion_date": self.ingestion_date,
            "checked_at": datetime.utcnow().isoformat(),
            "passes": self.passes,
            "warnings": self.warnings,
            "critical_failures": self.critical_failures,
            "checks": self.checks,
        }


# ──────────────────────────────────────────────────────────────
# BRONZE CHECKS
# ──────────────────────────────────────────────────────────────

def check_bronze(spark, ingestion_date):
    report = DQReport("bronze", ingestion_date)

    tables = {
        "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
        "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "price"],
        "order_payments": ["order_id", "payment_sequential", "payment_type", "payment_value"],
        "order_reviews": ["review_id", "order_id", "review_score"],
        "customers": ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
        "products": ["product_id", "product_category_name"],
        "sellers": ["seller_id", "seller_city", "seller_state"],
    }

    for table, expected_cols in tables.items():
        path = f"{BRONZE_BASE}/{table}/ingestion_date={ingestion_date}"

        # Check 1: Table exists
        try:
            df = spark.read.parquet(path)
            report.add(table, "exists", True)
        except Exception:
            report.add(table, "exists", False, detail=f"path not found: {path}")
            continue

        data_cols = [c for c in df.columns if not c.startswith("_")]

        # Check 2: Non-empty
        count = df.count()
        report.add(table, "non_empty", count > 0, detail=f"{count:,} rows")

        # Check 3: Expected columns present
        missing = [c for c in expected_cols if c not in data_cols]
        report.add(table, "schema_columns", len(missing) == 0,
                   detail=f"missing: {missing}" if missing else "all present")

        # Check 4: Metadata columns present
        meta_cols = ["_ingested_at", "_source_file", "_batch_id"]
        missing_meta = [c for c in meta_cols if c not in df.columns]
        report.add(table, "metadata_columns", len(missing_meta) == 0,
                   severity="WARN",
                   detail=f"missing: {missing_meta}" if missing_meta else "all present")

    return report


# ──────────────────────────────────────────────────────────────
# SILVER CHECKS
# ──────────────────────────────────────────────────────────────

def check_silver(spark, ingestion_date):
    report = DQReport("silver", ingestion_date)

    def read(table):
        try:
            return spark.read.parquet(f"{SILVER_BASE}/{table}/ingestion_date={ingestion_date}")
        except Exception:
            return None

    # ── Orders ──
    orders = read("orders")
    if orders is None:
        report.add("orders", "exists", False)
    else:
        count = orders.count()
        report.add("orders", "non_empty", count > 0, detail=f"{count:,} rows")

        # PK uniqueness
        dupes = count - orders.select("order_id").distinct().count()
        report.add("orders", "pk_unique(order_id)", dupes == 0,
                   detail=f"{dupes:,} duplicates")

        # Null PK
        nulls = orders.filter(F.col("order_id").isNull()).count()
        report.add("orders", "pk_not_null", nulls == 0, detail=f"{nulls:,} nulls")

        # Valid order status
        invalid = orders.filter(~F.col("order_status").isin(VALID_ORDER_STATUSES)).count()
        report.add("orders", "valid_order_status", invalid == 0,
                   severity="WARN", detail=f"{invalid:,} invalid")

        # purchase timestamp not null
        null_ts = orders.filter(F.col("order_purchase_timestamp").isNull()).count()
        report.add("orders", "purchase_timestamp_not_null", null_ts == 0,
                   detail=f"{null_ts:,} nulls")

        # delivery_date >= purchase_date (where both exist)
        delivered = orders.filter(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_purchase_timestamp").isNotNull()
        )
        bad_dates = delivered.filter(
            F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp")
        ).count()
        report.add("orders", "delivery_after_purchase", bad_dates == 0,
                   severity="WARN", detail=f"{bad_dates:,} violations")

    # ── Order Items ──
    items = read("order_items")
    if items is None:
        report.add("order_items", "exists", False)
    else:
        count = items.count()
        report.add("order_items", "non_empty", count > 0, detail=f"{count:,} rows")

        dupes = count - items.select("order_id", "order_item_id").distinct().count()
        report.add("order_items", "pk_unique", dupes == 0, detail=f"{dupes:,} duplicates")

        neg_price = items.filter(F.col("price") < 0).count()
        report.add("order_items", "price >= 0", neg_price == 0,
                   detail=f"{neg_price:,} negative")

        neg_freight = items.filter(F.col("freight_value") < 0).count()
        report.add("order_items", "freight >= 0", neg_freight == 0,
                   detail=f"{neg_freight:,} negative")

        null_product = items.filter(F.col("product_id").isNull()).count()
        report.add("order_items", "product_id_not_null", null_product == 0,
                   detail=f"{null_product:,} nulls")

    # ── Payments ──
    payments = read("order_payments")
    if payments is None:
        report.add("order_payments", "exists", False)
    else:
        count = payments.count()
        report.add("order_payments", "non_empty", count > 0, detail=f"{count:,} rows")

        neg_val = payments.filter(F.col("payment_value") < 0).count()
        report.add("order_payments", "payment_value >= 0", neg_val == 0,
                   detail=f"{neg_val:,} negative")

        invalid_type = payments.filter(~F.col("payment_type").isin(VALID_PAYMENT_TYPES)).count()
        report.add("order_payments", "valid_payment_type", invalid_type == 0,
                   severity="WARN", detail=f"{invalid_type:,} invalid")

    # ── Reviews ──
    reviews = read("order_reviews")
    if reviews is None:
        report.add("order_reviews", "exists", False)
    else:
        count = reviews.count()
        report.add("order_reviews", "non_empty", count > 0, detail=f"{count:,} rows")

        dupes = count - reviews.select("review_id").distinct().count()
        report.add("order_reviews", "pk_unique(review_id)", dupes == 0,
                   detail=f"{dupes:,} duplicates")

        bad_score = reviews.filter(~F.col("review_score").between(1, 5)).count()
        report.add("order_reviews", "review_score 1-5", bad_score == 0,
                   detail=f"{bad_score:,} out of range")

    # ── Customers ──
    customers = read("customers")
    if customers is None:
        report.add("customers", "exists", False)
    else:
        count = customers.count()
        report.add("customers", "non_empty", count > 0, detail=f"{count:,} rows")

        dupes = count - customers.select("customer_id").distinct().count()
        report.add("customers", "pk_unique(customer_id)", dupes == 0,
                   detail=f"{dupes:,} duplicates")

        invalid_state = customers.filter(~F.col("customer_state").isin(VALID_STATES)).count()
        report.add("customers", "valid_state", invalid_state == 0,
                   severity="WARN", detail=f"{invalid_state:,} invalid")

    # ── Referential integrity ──
    if all(df is not None for df in [orders, items, payments, reviews, customers]):
        products = read("products")
        sellers = read("sellers")

        ref_checks = [
            ("order_items→orders", items, "order_id", orders, "order_id"),
            ("orders→customers", orders, "customer_id", customers, "customer_id"),
            ("order_payments→orders", payments, "order_id", orders, "order_id"),
            ("order_reviews→orders", reviews, "order_id", orders, "order_id"),
        ]
        if products is not None:
            ref_checks.append(("order_items→products", items, "product_id", products, "product_id"))
        if sellers is not None:
            ref_checks.append(("order_items→sellers", items, "seller_id", sellers, "seller_id"))

        for label, child, child_col, parent, parent_col in ref_checks:
            orphans = child.join(
                parent.select(parent_col).distinct(),
                child[child_col] == parent[parent_col],
                "left_anti",
            ).count()
            report.add("referential", label, orphans == 0,
                       severity="WARN", detail=f"{orphans:,} orphans")

    # ── Source-to-target reconciliation ──
    if orders is not None:
        try:
            bronze_orders = spark.read.parquet(f"{BRONZE_BASE}/orders/ingestion_date={ingestion_date}")
            bronze_count = bronze_orders.count()
            data_cols = [c for c in bronze_orders.columns if not c.startswith("_")]
            bronze_data = bronze_orders.select(data_cols).count()
            silver_count = orders.count()
            diff = abs(bronze_data - silver_count)
            report.add("reconciliation", "orders_count_match",
                       diff == 0, severity="WARN",
                       detail=f"bronze={bronze_data:,} silver={silver_count:,} diff={diff:,}")
        except Exception:
            pass

    return report


# ──────────────────────────────────────────────────────────────
# GOLD / WAREHOUSE CHECKS
# ──────────────────────────────────────────────────────────────

def check_gold(spark, ingestion_date):
    report = DQReport("gold", ingestion_date)

    def read_gold(name):
        try:
            return spark.read.parquet(f"{GOLD_BASE}/{name}")
        except Exception:
            return None

    # ── Dimension checks ──
    dims = {
        "dim_date": ("date_sk", 1000),       # at least 1000 days
        "dim_customer": ("customer_sk", 90000),
        "dim_product": ("product_sk", 30000),
        "dim_seller": ("seller_sk", 3000),
        "dim_geography": ("geography_sk", 15000),
        "dim_payment_type": ("payment_type_sk", 4),
    }

    for dim_name, (sk_col, min_rows) in dims.items():
        df = read_gold(dim_name)
        if df is None:
            report.add(dim_name, "exists", False)
            continue

        count = df.count()
        report.add(dim_name, "non_empty", count >= min_rows,
                   detail=f"{count:,} rows (min {min_rows:,})")

        # SK uniqueness
        dupes = count - df.select(sk_col).distinct().count()
        report.add(dim_name, "sk_unique", dupes == 0, detail=f"{dupes:,} duplicates")

        # No null SKs
        nulls = df.filter(F.col(sk_col).isNull()).count()
        report.add(dim_name, "sk_not_null", nulls == 0, detail=f"{nulls:,} nulls")

    # ── Fact table checks ──
    facts = {
        "fact_order_items": {
            "grain": ["order_id", "order_item_id"],
            "sks": ["customer_sk", "product_sk", "seller_sk", "purchase_date_sk"],
            "measures": [("price", ">=0"), ("freight_value", ">=0")],
        },
        "fact_orders": {
            "grain": ["order_id"],
            "sks": ["customer_sk", "purchase_date_sk"],
            "measures": [("order_value", ">=0"), ("freight_total", ">=0")],
        },
        "fact_payments": {
            "grain": ["order_id", "payment_sequential"],
            "sks": ["customer_sk", "payment_type_sk", "payment_date_sk"],
            "measures": [("payment_value", ">=0")],
        },
        "fact_reviews": {
            "grain": ["review_id"],
            "sks": ["customer_sk"],
            "measures": [("review_score", "1-5")],
        },
    }

    for fact_name, config in facts.items():
        df = read_gold(fact_name)
        if df is None:
            report.add(fact_name, "exists", False)
            continue

        count = df.count()
        report.add(fact_name, "non_empty", count > 0, detail=f"{count:,} rows")

        # Grain uniqueness
        grain_cols = config["grain"]
        grain_dupes = count - df.select(grain_cols).distinct().count()
        report.add(fact_name, f"grain_unique({','.join(grain_cols)})",
                   grain_dupes == 0, detail=f"{grain_dupes:,} violations")

        # SK resolution (null check)
        for sk in config["sks"]:
            if sk in df.columns:
                nulls = df.filter(F.col(sk).isNull()).count()
                pct = (nulls / count * 100) if count > 0 else 0
                # Allow up to 1% null SKs (late-arriving dimension handling)
                report.add(fact_name, f"{sk}_resolved",
                           pct < 1.0, severity="WARN",
                           detail=f"{nulls:,} nulls ({pct:.2f}%)")

        # Measure validations
        for measure, rule in config["measures"]:
            if measure not in df.columns:
                continue
            if rule == ">=0":
                bad = df.filter(F.col(measure) < 0).count()
                report.add(fact_name, f"{measure} >= 0", bad == 0,
                           detail=f"{bad:,} negative")
            elif rule == "1-5":
                bad = df.filter(~F.col(measure).between(1, 5)).count()
                report.add(fact_name, f"{measure} in [1,5]", bad == 0,
                           detail=f"{bad:,} out of range")

    # ── SCD Type 2 integrity ──
    scd2 = read_gold("dim_customer_scd2")
    if scd2 is not None:
        unique_customers = scd2.select("customer_id").distinct().count()

        # Exactly one current record per customer
        current_counts = scd2.filter(F.col("is_current")).groupBy("customer_id").count()
        multi_current = current_counts.filter(F.col("count") > 1).count()
        report.add("dim_customer_scd2", "one_current_per_customer",
                   multi_current == 0, detail=f"{multi_current:,} with >1 current")

        no_current = unique_customers - current_counts.count()
        report.add("dim_customer_scd2", "all_have_current",
                   no_current == 0, detail=f"{no_current:,} missing current")

        # No overlapping date ranges
        overlaps = scd2.alias("a").join(
            scd2.alias("b"),
            (F.col("a.customer_id") == F.col("b.customer_id"))
            & (F.col("a.customer_sk") != F.col("b.customer_sk"))
            & (F.col("a.effective_from") <= F.col("b.effective_to"))
            & (F.col("a.effective_to") >= F.col("b.effective_from")),
        ).count()
        report.add("dim_customer_scd2", "no_overlapping_ranges",
                   overlaps == 0, detail=f"{overlaps:,} overlaps")

        # Valid segment values
        valid_segments = ["Bronze", "Silver", "Gold", "Platinum"]
        bad_seg = scd2.filter(~F.col("customer_segment").isin(valid_segments)).count()
        report.add("dim_customer_scd2", "valid_segments",
                   bad_seg == 0, detail=f"{bad_seg:,} invalid")

    return report


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 3:
        print("Usage: data_quality.py <ingestion_date> <bronze|silver|gold|all>")
        sys.exit(1)

    ingestion_date = sys.argv[1]
    layer = sys.argv[2]

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    reports = []
    all_pass = True

    if layer in ("bronze", "all"):
        r = check_bronze(spark, ingestion_date)
        reports.append(r)
    if layer in ("silver", "all"):
        r = check_silver(spark, ingestion_date)
        reports.append(r)
    if layer in ("gold", "all"):
        r = check_gold(spark, ingestion_date)
        reports.append(r)

    for r in reports:
        passed = r.print_report()
        if not passed:
            all_pass = False

    # Print combined JSON summary
    combined = {
        "ingestion_date": ingestion_date,
        "checked_at": datetime.utcnow().isoformat(),
        "overall_pass": all_pass,
        "layers": [r.to_json() for r in reports],
    }
    print(f"\n  DQ_REPORT_JSON={json.dumps({'pass': all_pass, 'critical': sum(r.critical_failures for r in reports), 'warnings': sum(r.warnings for r in reports)})}")

    spark.stop()

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
