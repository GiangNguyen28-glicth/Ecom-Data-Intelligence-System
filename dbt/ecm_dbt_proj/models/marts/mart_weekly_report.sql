{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['id', 'period_type', 'shard'],
        engine='ReplacingMergeTree()',
        alias='product_item_report_metrics',
        order_by=['id', 'period_type', 'shard'],
        partition_by="toYYYYMM(toDate(last_crawled_at))"
    )
}}

WITH source AS (
    SELECT *
    FROM report.product_daily_stats_staging
),

filtered AS (
    SELECT *
    FROM source
    WHERE 1=1
    {% if var('from_date', none) is not none %}
        AND crawledDateMs >= toDateTime('{{ var("from_date") }}')
    {% endif %}

    {% if var('to_date', none) is not none %}
        AND crawledDateMs < toDateTime('{{ var("to_date") }}')
    {% endif %}
),

weekly_aggregated AS (

    SELECT
        id,

        -- 👇 unified schema
        'week' AS period_type,
        toStartOfWeek(crawledDateMs) AS period_start,
        toISOWeek(period_start) AS shard,
        -- metrics
        SUM(dailySold) AS total_sold,
        SUM(gmv) AS total_gmv,

        AVG(price) AS avg_price,
        AVG(sellPrice) AS avg_sell_price,

        MAX(crawledDateMs) AS last_crawled_at

    FROM filtered

    GROUP BY
        id,
        period_start
)

-- ============================================================
-- FINAL
-- ============================================================
SELECT
    id,
    period_type,
    shard,
    period_start,
    last_crawled_at,
    total_sold,
    total_gmv,
    avg_price,
    avg_sell_price,
    now() AS created_at
FROM weekly_aggregated