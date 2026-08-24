-- PostgreSQL OLTP-style normalized schema
-- For comparison with ClickHouse OLAP star schema

CREATE SCHEMA IF NOT EXISTS oltp;

DROP TABLE IF EXISTS oltp.order_reviews CASCADE;
DROP TABLE IF EXISTS oltp.order_payments CASCADE;
DROP TABLE IF EXISTS oltp.order_items CASCADE;
DROP TABLE IF EXISTS oltp.orders CASCADE;
DROP TABLE IF EXISTS oltp.customers CASCADE;
DROP TABLE IF EXISTS oltp.products CASCADE;
DROP TABLE IF EXISTS oltp.sellers CASCADE;

CREATE TABLE oltp.customers (
    customer_id         VARCHAR(64) PRIMARY KEY,
    customer_unique_id  VARCHAR(64),
    zip_prefix          VARCHAR(10),
    city                VARCHAR(100),
    state               VARCHAR(2)
);

CREATE TABLE oltp.products (
    product_id          VARCHAR(64) PRIMARY KEY,
    category            VARCHAR(100),
    weight_g            INTEGER,
    length_cm           INTEGER,
    height_cm           INTEGER,
    width_cm            INTEGER
);

CREATE TABLE oltp.sellers (
    seller_id           VARCHAR(64) PRIMARY KEY,
    zip_prefix          VARCHAR(10),
    city                VARCHAR(100),
    state               VARCHAR(2)
);

CREATE TABLE oltp.orders (
    order_id            VARCHAR(64) PRIMARY KEY,
    customer_id         VARCHAR(64) REFERENCES oltp.customers(customer_id),
    order_status        VARCHAR(20),
    purchase_timestamp  TIMESTAMP,
    delivered_date      TIMESTAMP,
    estimated_date      TIMESTAMP
);

CREATE TABLE oltp.order_items (
    order_id            VARCHAR(64) REFERENCES oltp.orders(order_id),
    order_item_id       INTEGER,
    product_id          VARCHAR(64) REFERENCES oltp.products(product_id),
    seller_id           VARCHAR(64) REFERENCES oltp.sellers(seller_id),
    price               DOUBLE PRECISION,
    freight_value       DOUBLE PRECISION,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE oltp.order_payments (
    order_id            VARCHAR(64) REFERENCES oltp.orders(order_id),
    payment_sequential  INTEGER,
    payment_type        VARCHAR(20),
    installments        INTEGER,
    payment_value       DOUBLE PRECISION,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE oltp.order_reviews (
    review_id           VARCHAR(64) PRIMARY KEY,
    order_id            VARCHAR(64) REFERENCES oltp.orders(order_id),
    review_score        INTEGER,
    review_created      TIMESTAMP
);

CREATE INDEX idx_orders_purchase ON oltp.orders(purchase_timestamp);
CREATE INDEX idx_orders_customer ON oltp.orders(customer_id);
CREATE INDEX idx_items_product ON oltp.order_items(product_id);
CREATE INDEX idx_items_seller ON oltp.order_items(seller_id);
