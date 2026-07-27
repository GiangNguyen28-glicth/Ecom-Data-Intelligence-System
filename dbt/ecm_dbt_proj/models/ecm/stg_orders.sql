{{ config(materialized='view') }}

SELECT
    CAST(order_id AS UInt32) AS order_id,
    CAST(customer_id AS UInt32) AS customer_id,
    order_date,
    lower(status) AS status
FROM dbt_practice.orders
WHERE order_id IS NOT NULL