{{ config(materialized='view') }}

SELECT
    CAST(order_item_id AS UInt32) AS order_item_id,
    CAST(order_id AS UInt32) AS order_id,
    CAST(product_id AS UInt32) AS product_id,
    CAST(quantity AS UInt16) AS quantity
FROM dbt_practice.order_items
WHERE order_item_id IS NOT NULL