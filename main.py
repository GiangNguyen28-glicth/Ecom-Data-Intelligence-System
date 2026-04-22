# This is a sample Python script.
from pyspark.sql.functions import col, max, year, dayofmonth, month
from sqlalchemy.sql.ddl import CreateTable

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from adapters.minio_spark_adapter import minio_spark_adapter
from common.constants import PRODUCT_ITEM_DAILY_TABLE, LAST_STATE_PRODUCT_ITEM_TABLE, PRODUCT_ITEM_DAILY_REVENUE_TABLE, \
    PARSED_BUCKET
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
from schema.report import product_item_daily_schema


# Press Ctrl+F5 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press F9 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # dataTransferInstance = DataTransfer()
    transform_raw_data = TransformRawData()
    transform_raw_data.load()
    # migrate_parsed_data = MigrateParsedData()
    # migrate_parsed_data.migrate_parsed_data()
    # calc_revenue = CalcRevenue()
    # create_report_table = CreateReportTable()
    # create_report_table.create_database()
    # create_report_table.create_pid_table()
    # create_report_table.create_revenue_table()
    # create_report_table.create_state_table()
    # iceberg_2_kafka = Iceberg2Kafka()
    # iceberg_2_kafka.load_pid_revenue()
    # create_report_table.create_revenue_table()
    # calc_revenue.calc_daily_sold()
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
    # nasaInstance.load_df().select(col("crawledDateMs"), year(col("crawledDateMs")).alias("year")).show(1)
    # nasaInstance.load_df().withColumn("crawled_date", (col("crawledDateMs") / 1000).cast("timestamp")).select("crawled_date")
    # nasaInstance.created_database()
    # nasaInstance.created_table()
    # nasaInstance.write_demo_data()
    # nasaInstance.show_data()
    # nasaInstance.main()

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
