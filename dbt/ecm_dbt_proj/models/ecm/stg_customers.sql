{{config(materialized="view")}}
SELECT
    CAST(customer_id AS UInt32) as customer_id,
    trim(name) as customer_name,
    lower(email) as email,
    created_at
FROM dbt_practice.customers
WHERE customer_id IS NOT NULL