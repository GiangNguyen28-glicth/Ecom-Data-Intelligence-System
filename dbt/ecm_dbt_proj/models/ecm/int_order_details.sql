{{ config(materialized='view') }}

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,

    oi.order_item_id,
    oi.product_id,
    oi.quantity,

    p.product_name,
    p.category,
    p.price,

    oi.quantity * p.price AS gmv

FROM {{ ref('stg_orders') }} o
INNER JOIN {{ ref('stg_order_items') }} oi
    ON o.order_id = oi.order_id
INNER JOIN {{ ref('stg_products') }} p
    ON oi.product_id = p.product_id