-- Load Gold Parquet data from MinIO into ClickHouse
-- Uses ClickHouse's native S3 table function for direct Parquet reads
-- Truncate + insert for idempotent reloads

-- ════════════════════════════════════════════════════════════
-- DIMENSIONS
-- ════════════════════════════════════════════════════════════

TRUNCATE TABLE ecommerce.dim_date;
INSERT INTO ecommerce.dim_date
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/dim_date/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.dim_customer;
INSERT INTO ecommerce.dim_customer
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/dim_customer/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.dim_customer_scd2;
INSERT INTO ecommerce.dim_customer_scd2
SELECT
    customer_sk, customer_id, customer_unique_id,
    zip_prefix, city, state, latitude, longitude,
    customer_segment, cumulative_spend,
    toDate32(effective_from),
    if(effective_to > 120000, toDate32('2299-12-31'), toDate32(effective_to)),
    is_current
FROM s3('http://minio:9000/ecommerce-data/gold/dim_customer_scd2/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet',
  'customer_sk Int64, customer_id String, customer_unique_id String,
   zip_prefix Nullable(String), city Nullable(String), state Nullable(String),
   latitude Nullable(Float64), longitude Nullable(Float64),
   customer_segment String, cumulative_spend Float64,
   effective_from Date32, effective_to Int32, is_current Bool');

TRUNCATE TABLE ecommerce.dim_product;
INSERT INTO ecommerce.dim_product
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/dim_product/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.dim_seller;
INSERT INTO ecommerce.dim_seller
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/dim_seller/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.dim_geography;
INSERT INTO ecommerce.dim_geography
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/dim_geography/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.dim_payment_type;
INSERT INTO ecommerce.dim_payment_type
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/dim_payment_type/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

-- ════════════════════════════════════════════════════════════
-- FACT TABLES
-- ════════════════════════════════════════════════════════════

TRUNCATE TABLE ecommerce.fact_order_items;
INSERT INTO ecommerce.fact_order_items
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/fact_order_items/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.fact_orders;
INSERT INTO ecommerce.fact_orders
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/fact_orders/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.fact_payments;
INSERT INTO ecommerce.fact_payments
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/fact_payments/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');

TRUNCATE TABLE ecommerce.fact_reviews;
INSERT INTO ecommerce.fact_reviews
SELECT * FROM s3('http://minio:9000/ecommerce-data/gold/fact_reviews/*.parquet',
  'minio_access_key', 'minio_secret_key', 'Parquet');
