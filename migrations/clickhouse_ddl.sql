CREATE DATABASE report

CREATE TABLE IF NOT EXISTS report.product_daily_stats_staging (
    id String,
    dailySold UInt64,
    price Decimal(18, 2),
    sellPrice Decimal(18, 2),
    gmv Float64,
    crawledDateMs DateTime64(3, 'UTC'),
    createdDate DateTime64(3, 'UTC'),
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(crawledDateMs)
ORDER BY (id, crawledDateMs);

CREATE TABLE report.clickhouse_ingest_topic_staging (
    data String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'host.docker.internal:9092',
    kafka_topic_list = 'clickhouse_ingest_topic_staging',
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'RawBLOB'

CREATE MATERIALIZED VIEW report.kafka_to_stats_mv_staging
TO report.product_daily_stats_staging AS
SELECT
    JSONExtractString(raw_item, 'id') AS id,
    JSONExtractUInt(raw_item, 'dailySold') AS dailySold,
    toDecimal64(JSONExtractFloat(raw_item, 'price'), 2) AS price,
    toDecimal64(JSONExtractFloat(raw_item, 'sellPrice'), 2) AS sellPrice,
    JSONExtractFloat(raw_item, 'gmv') AS gmv,
    parseDateTime64BestEffort(JSONExtractString(raw_item, 'crawledDateMs'), 3) AS crawledDateMs,
    parseDateTime64BestEffort(JSONExtractString(raw_item, 'createdDate'), 3) AS createdDate
FROM (
    SELECT arrayJoin(JSONExtractArrayRaw(data)) AS raw_item
    FROM report.clickhouse_ingest_topic_staging
);

--DROP VIEW report.kafka_to_stats_mv;