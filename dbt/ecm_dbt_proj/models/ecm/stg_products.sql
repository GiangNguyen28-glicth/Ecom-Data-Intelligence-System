{{ config(materialized='view') }}

SELECT
    CAST(product_id AS UInt32) AS product_id,
    trim(name) AS product_name,
    trim(category) AS category,
    CAST(price AS Decimal(12,2)) AS price
FROM dbt_practice.products
WHERE product_id IS NOT NULL