CREATE DATABASE IF NOT EXISTS dbt_practice;

CREATE TABLE dbt_practice.customers
(
    customer_id UInt32,
    name String,
    email String,
    created_at Date
)
ENGINE = MergeTree
ORDER BY customer_id;

CREATE TABLE dbt_practice.products
(
    product_id UInt32,
    name String,
    category String,
    price Decimal(12,2)
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE dbt_practice.orders
(
    order_id UInt32,
    customer_id UInt32,
    order_date Date,
    status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (order_date, order_id);

CREATE TABLE dbt_practice.order_items
(
    order_item_id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity UInt16
)
ENGINE = MergeTree
ORDER BY (order_id, product_id);