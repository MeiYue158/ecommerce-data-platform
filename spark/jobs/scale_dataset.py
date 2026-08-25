"""
Dataset Scale-Up

Multiplies existing Silver data by N copies with deterministic ID remapping.
Maintains referential integrity across all copies.

For copy n, new_id = md5(original_id || n) — deterministic and FK-consistent.

Usage:
    spark-submit scale_dataset.py <source_date> <target_date> <scale_factor>
    spark-submit scale_dataset.py 2026-08-24 2026-08-24-10x 10
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


SILVER_BASE = "s3a://ecommerce-data/silver"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("scale_dataset")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def remap_id(col_name, copy_num_col="copy_n"):
    """Generate deterministic new ID: md5(original_id || copy_number)."""
    return F.md5(F.concat(F.col(col_name), F.lit("_"), F.col(copy_num_col)))


def scale_table(spark, table, source_date, target_date, scale_factor,
                id_columns, remap_columns, jitter_columns=None):
    """
    Read a Silver table, create scale_factor copies with remapped IDs.

    id_columns: columns to remap with new unique IDs
    remap_columns: FK columns to remap (same function, ensuring FK consistency)
    jitter_columns: numeric columns to add small random variation
    """
    source = f"{SILVER_BASE}/{table}/ingestion_date={source_date}"
    target = f"{SILVER_BASE}/{table}/ingestion_date={target_date}"

    df = spark.read.parquet(source)
    original_count = df.count()

    # Create copy numbers: 0 = original, 1..N-1 = copies
    copies = spark.range(0, scale_factor).withColumnRenamed("id", "copy_n")
    scaled = df.crossJoin(copies)

    # Remap ID columns (PKs)
    for col in id_columns:
        scaled = scaled.withColumn(
            col,
            F.when(F.col("copy_n") == 0, F.col(col))  # keep original for copy 0
            .otherwise(remap_id(col))
        )

    # Remap FK columns (must match parent table's remapping)
    for col in remap_columns:
        scaled = scaled.withColumn(
            col,
            F.when(F.col("copy_n") == 0, F.col(col))
            .otherwise(remap_id(col))
        )

    # Add jitter to numeric columns for realism
    if jitter_columns:
        for col in jitter_columns:
            scaled = scaled.withColumn(
                col,
                F.when(F.col("copy_n") == 0, F.col(col))
                .otherwise(
                    F.round(F.col(col) * (0.8 + F.rand() * 0.4), 2)  # ±20% variation
                )
            )

    # Jitter timestamps for non-copy-0
    ts_cols = [c for c in df.columns if "timestamp" in c.lower() or "date" in c.lower()]
    for col in ts_cols:
        if df.schema[col].dataType.typeName() in ("timestamp",):
            scaled = scaled.withColumn(
                col,
                F.when(F.col("copy_n") == 0, F.col(col))
                .otherwise(
                    F.col(col) + F.expr("INTERVAL 1 HOUR") * (F.rand() * 48 - 24).cast("int")
                )
            )

    scaled = scaled.drop("copy_n")
    final_count = scaled.count()

    scaled.write.mode("overwrite").parquet(target)
    print(f"  [{table}] {original_count:,} x {scale_factor} = {final_count:,} rows")
    return final_count


def main():
    if len(sys.argv) < 4:
        print("Usage: scale_dataset.py <source_date> <target_date> <scale_factor>")
        sys.exit(1)

    source_date = sys.argv[1]
    target_date = sys.argv[2]
    scale_factor = int(sys.argv[3])

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  DATASET SCALE-UP")
    print(f"  Source: {source_date}  Target: {target_date}  Factor: {scale_factor}x")
    print(f"{'='*60}\n")

    total = 0

    # Scale customers first (parent table)
    total += scale_table(spark, "customers", source_date, target_date, scale_factor,
                         id_columns=["customer_id"],
                         remap_columns=["customer_unique_id"])

    # Scale sellers (parent table — keep as-is, they're reference data)
    # Just copy without scaling to keep seller count realistic
    sellers = spark.read.parquet(f"{SILVER_BASE}/sellers/ingestion_date={source_date}")
    sellers.write.mode("overwrite").parquet(f"{SILVER_BASE}/sellers/ingestion_date={target_date}")
    print(f"  [sellers] {sellers.count():,} rows (reference data, not scaled)")

    # Scale products (keep as-is — reference data)
    products = spark.read.parquet(f"{SILVER_BASE}/products/ingestion_date={source_date}")
    products.write.mode("overwrite").parquet(f"{SILVER_BASE}/products/ingestion_date={target_date}")
    print(f"  [products] {products.count():,} rows (reference data, not scaled)")

    # Copy category_translation and geography (unchanged)
    for ref_table in ["category_translation", "geography"]:
        ref = spark.read.parquet(f"{SILVER_BASE}/{ref_table}/ingestion_date={source_date}")
        ref.write.mode("overwrite").parquet(f"{SILVER_BASE}/{ref_table}/ingestion_date={target_date}")
        print(f"  [{ref_table}] {ref.count():,} rows (reference data, not scaled)")

    # Scale orders (FK: customer_id must match scaled customers)
    total += scale_table(spark, "orders", source_date, target_date, scale_factor,
                         id_columns=["order_id"],
                         remap_columns=["customer_id"])

    # Scale order_items (FK: order_id must match scaled orders)
    # product_id and seller_id stay the same (reference data not scaled)
    total += scale_table(spark, "order_items", source_date, target_date, scale_factor,
                         id_columns=[],  # composite PK, order_id is remapped as FK
                         remap_columns=["order_id"],
                         jitter_columns=["price", "freight_value"])

    # Scale payments (FK: order_id)
    total += scale_table(spark, "order_payments", source_date, target_date, scale_factor,
                         id_columns=[],
                         remap_columns=["order_id"],
                         jitter_columns=["payment_value"])

    # Scale reviews (FK: order_id)
    total += scale_table(spark, "order_reviews", source_date, target_date, scale_factor,
                         id_columns=["review_id"],
                         remap_columns=["order_id"])

    print(f"\n  Total scaled rows: {total:,}")
    spark.stop()
    print("  Done.")


if __name__ == "__main__":
    main()
