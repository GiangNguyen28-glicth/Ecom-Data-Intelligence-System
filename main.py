# This is a sample Python script.
import clickhouse_connect
from pyspark.sql.functions import col, max, year, dayofmonth, month, to_date
from sqlalchemy.sql.ddl import CreateTable

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from adapters.minio_spark_adapter import minio_spark_adapter
from common.constants import PRODUCT_ITEM_DAILY_TABLE, LAST_STATE_PRODUCT_ITEM_TABLE, PRODUCT_ITEM_DAILY_REVENUE_TABLE, \
    PARSED_BUCKET, PRODUCT_ITEM_DAILY_REVENUE_STAGING_TABLE
from configs.settings import settings
from example.ecm.ecm_olist import ECMOlist
from example.nasa_http_log.nasa_http_log import NasaHttpLog
from example.unknown.iceberg import Iceberg
from example.unknown.unknown import Unknown
from helpers.helpers import Helper
from jobs.data_transfer import DataTransfer
from jobs.migrate_parsed_data import MigrateParsedData
from jobs.transform_raw_data import TransformRawData
from migrations.report import pid_revenue_ddl
from jobs.calc_revenue import CalcRevenue
from jobs.create_report_table import CreateReportTable
# from report.data_transfer import DataTransfer
from jobs.iceberg_2_kafka import Iceberg2Kafka
from schema.ecm_olist import customers_schema
from schema.report import product_item_daily_schema, raw_content_schema


# Press Ctrl+F5 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import  requests

def validate_data_sync_to_clickhouse(**kwargs):
    try:
        consumer_group_url = "/api/clusters/My%20Kafka%20Cluster/consumer-groups/clickhouse_consumer_group"
        res = requests.get(f"{settings.kafka_api_url}{consumer_group_url}", timeout=30)
        res.raise_for_status()
        partitions = res.json()["partitions"]
        print("Get kafka partitions")
        clickhouse_ingest_topic_staging_partition = list(filter(lambda p: p["topic"] == "clickhouse_ingest_topic_staging", partitions)).pop()
        if clickhouse_ingest_topic_staging_partition is None:
            return False
        print("Get clickhouse ingest topic staging partition")
        ch_client = clickhouse_connect.get_client(host=settings.clickhouse_host, database=settings.clickhouse_db,
                                                  port=settings.clickhouse_port, username=settings.clickhouse_username, password=settings.clickhouse_password)
        result_clickhouse = ch_client.query("""
                            SELECT
                                assignments.topic, assignments.current_offset
                            FROM system.kafka_consumers
                            WHERE database = 'report'
                              AND table = 'clickhouse_ingest_topic_staging'
                        """)
        result_clickhouse = list(result_clickhouse.named_results()).pop()
        if result_clickhouse is None:
            return False
        print(result_clickhouse)
        clickhouse_kafka_current_offset = result_clickhouse["assignments.current_offset"].pop()
        current_offset = clickhouse_ingest_topic_staging_partition["currentOffset"]
        end_offset = clickhouse_ingest_topic_staging_partition["endOffset"]
        print("Current offset", current_offset)
        print("End offset", end_offset)
        print("clickhouse_kafka_current_offset:", clickhouse_kafka_current_offset)
        is_kafka_sync_success = clickhouse_ingest_topic_staging_partition["currentOffset"] == clickhouse_ingest_topic_staging_partition["endOffset"]
        print("is_kafka_sync_success: {}".format(is_kafka_sync_success))
        if clickhouse_kafka_current_offset == -1001:
            print("Clickhouse inconsistencies. Use kafa info")
            return is_kafka_sync_success
        is_clickhouse_sync_success = clickhouse_kafka_current_offset == clickhouse_ingest_topic_staging_partition["endOffset"]
        is_sync_success = is_kafka_sync_success and is_clickhouse_sync_success
        print("is_sync_success: {}".format(is_sync_success))
        return is_sync_success
        print("clickhouse_kafka_current_offset: ",clickhouse_kafka_current_offset)
        print("clickhouse_ingest_topic_staging_partition: ", clickhouse_ingest_topic_staging_partition)
    except Exception as e:
        print("Exception:", e)
        return False

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # dataTransferInstance = DataTransfer()
    # transform_raw_data = TransformRawData()
    # config = {
    #     "from_date": "2026-04-24",
    #     "to_date": "2026-04-25"
    # }
    # transform_raw_data.transform_raw_2_parsed_data(config)
    # transform_raw_data.check_data()
    # migrate_parsed_data = MigrateParsedData()
    # migrate_parsed_data.migrate_parsed_data()
    # calc_revenue = CalcRevenue()
    # create_report_table = CreateReportTable()
    # create_report_table.create_database()
    # create_report_table.create_pid_table()
    # iceberg_spark_adapter.truncate_table(PRODUCT_ITEM_DAILY_REVENUE_STAGING_TABLE)
    # iceberg_spark_adapter.drop_table(PRODUCT_ITEM_DAILY_REVENUE_STAGING_TABLE)
    # create_report_table.create_revenue_table()
    # create_report_table.create_state_table()
    # iceberg_spark_adapter.spark.s
    # iceberg_2_kafka = Iceberg2Kafka()
    # iceberg_2_kafka.load_pid_revenue()
    # create_report_table.create_revenue_table()
    # calc_revenue.calc_daily_sold()
    # iceberg_spark_adapter.spark.read.schema(raw_content_schema).parquet("s3a://raw-data")
    # iceberg_spark_adapter.traverse_table(LAST_STATE_PRODUCT_ITEM_TABLE).filter(to_date(col("committed_at")) == "2026-04-21").show(10)
    # iceberg_spark_adapter.get_table_by_version(LAST_STATE_PRODUCT_ITEM_TABLE, "7826167569527212278").show(10)
    # iceberg_spark_adapter.truncate_table(PRODUCT_ITEM_DAILY_REVENUE_TABLE)
    # iceberg_spark_adapter.drop_table(PRODUCT_ITEM_DAILY_REVENUE_TABLE)
    # dataTransferInstance.transfer_to_iceberg_latest_pi()
    # current_date = Helper.get_current_date()
    # bucket = Helper.get_bucket(PARSED_BUCKET)
    # df = iceberg_spark_adapter.spark.read.parquet(f"s3a://parsed-data/2026/04/17/00de3f9e-f87d-4b3d-8ccf-103aa4cc3e68").show(1000)
    # iceberg_spark_adapter.spark.sql(f"SELECT * FROM {LAST_STATE_PRODUCT_ITEM_TABLE}").sort(col("crawledDateMs").asc()).show(10)
    # iceberg_spark_adapter.spark.sql("SELECT * FROM ecm_catalog.report.product_item_daily.files").show(truncate=False)
    # print(iceberg_spark_adapter.spark)
    # iceberg_spark_adapter.spark.sql(sql)
    # nasaInstance.load_df().withColumn("crawled_date", (col("crawledDateMs") / 1000).cast("timestamp")).select("crawled_date")
    # nasaInstance.created_database()
    # nasaInstance.created_table()
    # nasaInstance.write_demo_data()
    # nasaInstance.show_data()
    # nasaInstance.main()

    # consumer_group_url = "/api/clusters/My%20Kafka%20Cluster/consumer-groups/clickhouse_consumer_group"
    # res = requests.get(f"{settings.kafka_api_url}{consumer_group_url}", timeout=30)
    # res.raise_for_status()
    # partitions = res.json()["partitions"]
    # clickhouse_ingest_topic_staging_partition = list(filter(lambda p: p["topic"] == "clickhouse_ingest_topic_staging",
    #                                                    partitions))[0]
    # print(clickhouse_ingest_topic_staging_partition)

    # ch_client = clickhouse_connect.get_client(host=settings.clickhouse_host, database=settings.clickhouse_db,port=8123, username="default", password="default")
    # result = ch_client.query("""
    #                 SELECT
    #                     assignments.topic, assignments.current_offset
    #                 FROM system.kafka_consumers
    #                 WHERE database = 'report'
    #                   AND table = 'clickhouse_ingest_topic_staging'
    #             """)
    #
    # result = list(result.named_results())
    # clickhouse_ingest_topic_staging_row = result.pop()
    # current_offset = clickhouse_ingest_topic_staging_row["current_offset"].pop()
    # print(clickhouse_ingest_topic_staging_row["assignments.current_offset"].pop())
    validate_data_sync_to_clickhouse()
