-- ClickHouse OLAP Warehouse DDL
-- Star schema: 6 dimensions + 4 fact tables + 1 SCD2 dimension

CREATE DATABASE IF NOT EXISTS ecommerce;

-- ════════════════════════════════════════════════════════════
-- DIMENSIONS
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ecommerce.dim_date (
    date_sk          Int32,
    date             Date,
    year             Int32,
    quarter          Int32,
    month            Int32,
    month_name       String,
    week             Int32,
    day              Int32,
    day_of_week      Int32,
    day_name         String,
    is_weekend       Bool,
    year_month       String,
    year_quarter     String
) ENGINE = MergeTree()
ORDER BY date_sk;

CREATE TABLE IF NOT EXISTS ecommerce.dim_customer (
    customer_sk          Int64,
    customer_id          String,
    customer_unique_id   String,
    zip_prefix           Nullable(String),
    city                 Nullable(String),
    state                Nullable(String),
    latitude             Nullable(Float64),
    longitude            Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY customer_sk;

CREATE TABLE IF NOT EXISTS ecommerce.dim_customer_scd2 (
    customer_sk          Int64,
    customer_id          String,
    customer_unique_id   String,
    zip_prefix           Nullable(String),
    city                 Nullable(String),
    state                Nullable(String),
    latitude             Nullable(Float64),
    longitude            Nullable(Float64),
    customer_segment     String,
    cumulative_spend     Float64,
    effective_from       Date32,
    effective_to         Date32,
    is_current           Bool
) ENGINE = MergeTree()
ORDER BY (customer_id, effective_from);

CREATE TABLE IF NOT EXISTS ecommerce.dim_product (
    product_sk           Int64,
    product_id           String,
    category             Nullable(String),
    category_original    Nullable(String),
    weight_g             Nullable(Int32),
    length_cm            Nullable(Int32),
    height_cm            Nullable(Int32),
    width_cm             Nullable(Int32),
    photos_qty           Nullable(Int32),
    name_length          Nullable(Int32),
    description_length   Nullable(Int32)
) ENGINE = MergeTree()
ORDER BY product_sk;

CREATE TABLE IF NOT EXISTS ecommerce.dim_seller (
    seller_sk        Int64,
    seller_id        String,
    zip_prefix       Nullable(String),
    city             Nullable(String),
    state            Nullable(String),
    latitude         Nullable(Float64),
    longitude        Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY seller_sk;

CREATE TABLE IF NOT EXISTS ecommerce.dim_geography (
    geography_sk     Int64,
    zip_prefix       String,
    city             String,
    state            String,
    latitude         Float64,
    longitude        Float64,
    sample_count     Int64
) ENGINE = MergeTree()
ORDER BY geography_sk;

CREATE TABLE IF NOT EXISTS ecommerce.dim_payment_type (
    payment_type_sk  Int64,
    payment_type     String
) ENGINE = MergeTree()
ORDER BY payment_type_sk;

-- ════════════════════════════════════════════════════════════
-- FACT TABLES
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ecommerce.fact_order_items (
    order_id             String,
    order_item_id        Int32,
    customer_sk          Int64 DEFAULT 0,
    product_sk           Int64 DEFAULT 0,
    seller_sk            Int64 DEFAULT 0,
    purchase_date_sk     Int32 DEFAULT 0,
    delivery_date_sk     Int32 DEFAULT 0,
    shipping_limit_date_sk Int32 DEFAULT 0,
    price                Float64,
    freight_value        Float64,
    order_status         String
) ENGINE = MergeTree()
PARTITION BY intDiv(purchase_date_sk, 100)
ORDER BY (purchase_date_sk, order_id, order_item_id);

CREATE TABLE IF NOT EXISTS ecommerce.fact_orders (
    order_id                   String,
    customer_sk                Int64 DEFAULT 0,
    purchase_date_sk           Int32 DEFAULT 0,
    approved_date_sk           Int32 DEFAULT 0,
    delivered_carrier_date_sk  Int32 DEFAULT 0,
    delivery_date_sk           Int32 DEFAULT 0,
    estimated_delivery_date_sk Int32 DEFAULT 0,
    order_status               String,
    order_value                Float64,
    freight_total              Float64,
    item_count                 Int32,
    delivery_days              Int32 DEFAULT 0,
    estimated_delivery_days    Int32 DEFAULT 0,
    delivery_delay_days        Int32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY intDiv(purchase_date_sk, 100)
ORDER BY (purchase_date_sk, order_id);

CREATE TABLE IF NOT EXISTS ecommerce.fact_payments (
    order_id             String,
    payment_sequential   Int32,
    customer_sk          Int64 DEFAULT 0,
    payment_type_sk      Int64 DEFAULT 0,
    payment_date_sk      Int32 DEFAULT 0,
    installments         Int32,
    payment_value        Float64
) ENGINE = MergeTree()
PARTITION BY intDiv(payment_date_sk, 100)
ORDER BY (payment_date_sk, order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS ecommerce.fact_reviews (
    review_id                String,
    order_id                 String,
    customer_sk              Int64 DEFAULT 0,
    review_created_date_sk   Int32 DEFAULT 0,
    review_answer_date_sk    Int32 DEFAULT 0,
    review_score             Int32,
    review_comment_title     Nullable(String),
    review_comment_message   Nullable(String)
) ENGINE = MergeTree()
ORDER BY (review_created_date_sk, order_id);
