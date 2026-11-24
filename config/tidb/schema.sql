-- ==============================================================================
-- FerrousCX Canonical TiDB Transactional HTAP Schema (Story 23.1, AD-36)
-- Full 9-table Kaggle Brazilian E-Commerce Public Dataset by Olist
-- (olistbr/brazilian-ecommerce) + CQRS/ES Operational State & Benchmark Tables
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS ferrouscx;
USE ferrouscx;

-- 1. olist_orders_dataset.csv (Canonical Base Order Table, AD-34)
CREATE TABLE IF NOT EXISTS olist_orders (
    order_id VARCHAR(64) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    order_status VARCHAR(32) NOT NULL,
    order_purchase_timestamp DATETIME(6) NOT NULL,
    order_approved_at DATETIME(6) NULL,
    order_delivered_carrier_date DATETIME(6) NULL,
    order_delivered_customer_date DATETIME(6) NULL,
    order_estimated_delivery_date DATETIME(6) NOT NULL,
    INDEX idx_olist_orders_customer (customer_id),
    INDEX idx_olist_orders_status (order_status),
    INDEX idx_olist_orders_purchase_ts (order_purchase_timestamp)
);

-- 2. olist_order_items_dataset.csv
CREATE TABLE IF NOT EXISTS olist_order_items (
    order_id VARCHAR(64) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    seller_id VARCHAR(64) NOT NULL,
    shipping_limit_date DATETIME(6) NULL,
    price DOUBLE NOT NULL DEFAULT 0.0,
    freight_value DOUBLE NOT NULL DEFAULT 0.0,
    PRIMARY KEY (order_id, order_item_id),
    INDEX idx_olist_items_product (product_id),
    INDEX idx_olist_items_seller (seller_id)
);

-- 3. olist_order_payments_dataset.csv
CREATE TABLE IF NOT EXISTS olist_order_payments (
    order_id VARCHAR(64) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(32) NOT NULL,
    payment_installments INT NOT NULL DEFAULT 1,
    payment_value DOUBLE NOT NULL DEFAULT 0.0,
    PRIMARY KEY (order_id, payment_sequential)
);

-- 4. olist_order_reviews_dataset.csv
CREATE TABLE IF NOT EXISTS olist_order_reviews (
    review_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    review_score INT NOT NULL,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message TEXT NULL,
    review_creation_date DATETIME(6) NULL,
    review_answer_timestamp DATETIME(6) NULL,
    PRIMARY KEY (review_id, order_id),
    INDEX idx_olist_reviews_order (order_id),
    INDEX idx_olist_reviews_score (review_score)
);

-- 5. olist_customers_dataset.csv
CREATE TABLE IF NOT EXISTS olist_customers (
    customer_id VARCHAR(64) NOT NULL PRIMARY KEY,
    customer_unique_id VARCHAR(64) NOT NULL,
    customer_zip_code_prefix VARCHAR(16) NOT NULL,
    customer_city VARCHAR(128) NOT NULL,
    customer_state VARCHAR(8) NOT NULL,
    INDEX idx_olist_customers_unique (customer_unique_id),
    INDEX idx_olist_customers_zip (customer_zip_code_prefix)
);

-- 6. olist_products_dataset.csv (Preserves exact Kaggle CSV spelling: lenght)
CREATE TABLE IF NOT EXISTS olist_products (
    product_id VARCHAR(64) NOT NULL PRIMARY KEY,
    product_category_name VARCHAR(128) NULL,
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g DOUBLE NULL,
    product_length_cm DOUBLE NULL,
    product_height_cm DOUBLE NULL,
    product_width_cm DOUBLE NULL,
    INDEX idx_olist_products_category (product_category_name)
);

-- 7. olist_sellers_dataset.csv
CREATE TABLE IF NOT EXISTS olist_sellers (
    seller_id VARCHAR(64) NOT NULL PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(16) NOT NULL,
    seller_city VARCHAR(128) NOT NULL,
    seller_state VARCHAR(8) NOT NULL,
    INDEX idx_olist_sellers_zip (seller_zip_code_prefix)
);

-- 8. olist_geolocation_dataset.csv (Deduplicated & averaged per CEP prefix, AD-35)
CREATE TABLE IF NOT EXISTS olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(16) NOT NULL PRIMARY KEY,
    geolocation_lat DOUBLE NOT NULL,
    geolocation_lng DOUBLE NOT NULL,
    geolocation_city VARCHAR(128) NOT NULL,
    geolocation_state VARCHAR(8) NOT NULL
);

-- 9. product_category_name_translation.csv
CREATE TABLE IF NOT EXISTS olist_category_translation (
    product_category_name VARCHAR(128) NOT NULL PRIMARY KEY,
    product_category_name_english VARCHAR(128) NOT NULL
);

-- 10. CQRS Consolidated Order State (8 olist_orders base + 6 CQRS overlay + items/payments JSON)
CREATE TABLE IF NOT EXISTS orders_current_state (
    order_id VARCHAR(64) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    order_status VARCHAR(32) NOT NULL,
    order_purchase_timestamp DATETIME(6) NOT NULL,
    order_approved_at DATETIME(6) NULL,
    order_delivered_carrier_date DATETIME(6) NULL,
    order_delivered_customer_date DATETIME(6) NULL,
    order_estimated_delivery_date DATETIME(6) NOT NULL,
    total_amount_cents BIGINT NOT NULL DEFAULT 0,
    total_freight_cents BIGINT NOT NULL DEFAULT 0,
    courtesy_credit_cents BIGINT NOT NULL DEFAULT 0,
    credit_delta BIGINT NOT NULL DEFAULT 0,
    version BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL,
    items_json JSON NULL,
    payments_json JSON NULL,
    INDEX idx_orders_state_customer (customer_id),
    INDEX idx_orders_state_status (order_status)
);

-- 11. Raw Domain Event Audit Trail (orders/intake)
CREATE TABLE IF NOT EXISTS orders_raw_events (
    event_id VARCHAR(64) NOT NULL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL DEFAULT '',
    event_type VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    ingest_timestamp DATETIME(6) NOT NULL,
    INDEX idx_raw_events_order (order_id)
);

-- 12. HPC-MQBench Live E2E Qualification Table
CREATE TABLE IF NOT EXISTS hpc_benchmarks (
    run_id VARCHAR(64) NOT NULL,
    record_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL DEFAULT 0,
    partition_id INT NOT NULL DEFAULT 0,
    client_timestamp_us BIGINT UNSIGNED NOT NULL DEFAULT 0,
    iggy_timestamp_us BIGINT UNSIGNED NOT NULL DEFAULT 0,
    commit_timestamp_us BIGINT UNSIGNED NOT NULL DEFAULT 0,
    payload_size_bytes BIGINT NOT NULL DEFAULT 0,
    data TEXT NULL,
    PRIMARY KEY (run_id, record_id),
    INDEX idx_hpc_run (run_id)
);

-- 13. Transactional Iggy Consumer Offset Checkpoints (ACID with batch UPSERTs)
CREATE TABLE IF NOT EXISTS _iggy_sink_offsets (
    sink_name VARCHAR(64) NOT NULL,
    partition_id INT UNSIGNED NOT NULL,
    next_offset BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (sink_name, partition_id)
);
