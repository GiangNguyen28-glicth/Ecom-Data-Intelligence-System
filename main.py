# This is a sample Python script.
from pyspark.sql.functions import col, max, year, dayofmonth, month
from sqlalchemy.sql.ddl import CreateTable

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import PRODUCT_ITEM_DAILY_TABLE, LAST_STATE_PRODUCT_ITEM_TABLE, PRODUCT_ITEM_DAILY_REVENUE_TABLE
from example.ecm.ecm_olist import ECMOlist
from example.nasa_http_log.nasa_http_log import NasaHttpLog
from example.unknown.iceberg import Iceberg
from example.unknown.unknown import Unknown
from migrations.report import pid_revenue_ddl
from report.calc_revenue import CalcRevenue
from report.create_report_table import CreateReportTable
from report.data_transfer import DataTransfer
from report.iceberg_2_kafka import Iceberg2Kafka
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
    # calc_revenue = CalcRevenue()
    # create_report_table = CreateReportTable()
    iceberg_2_kafka = Iceberg2Kafka()
    iceberg_2_kafka.load_pid_revenue()
    # create_report_table.create_revenue_table()
    # calc_revenue.calc_daily_sold()
    # iceberg_spark_adapter.truncate_table(PRODUCT_ITEM_DAILY_REVENUE_TABLE)
    # iceberg_spark_adapter.drop_table(PRODUCT_ITEM_DAILY_REVENUE_TABLE)
    # dataTransferInstance.transfer_to_iceberg_latest_pi()
    # iceberg_spark_adapter.spark.sql(f"SELECT * FROM {PRODUCT_ITEM_DAILY_REVENUE_TABLE}").show()
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
