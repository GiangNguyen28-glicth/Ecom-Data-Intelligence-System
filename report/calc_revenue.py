from pyspark.sql.functions import col, broadcast, current_timestamp

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import LAST_STATE_PRODUCT_ITEM_TABLE, PRODUCT_ITEM_DAILY_TABLE, PRODUCT_ITEM_DAILY_REVENUE_TABLE
from helpers.helpers import Helper


class CalcRevenue:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def calc_daily_sold(self):
        now = current_timestamp()
        now.day = 17
        current_date = Helper.get_current_date()
        process_date = f"{current_date['year']}-{current_date['month']}-{current_date['day']}"
        # process_date = f"{current_date['year']}-{current_date['month']}-17"
        print("Process date:", process_date)
        daily_df = self.spark.read.table(PRODUCT_ITEM_DAILY_TABLE).filter(col("crawledDateMs") >= process_date)
        daily_df.show(10)
        state_df = self.spark.read.table(LAST_STATE_PRODUCT_ITEM_TABLE)
        state_df.show(10)
        joined_df = daily_df.alias("daily").join(
            broadcast(state_df).alias("state"),
            col("daily.id") == col("state.id"),
            "left"
        )
        result_df = (
            joined_df
            .filter(
                col("daily.sold") > col("state.sold")
            )
            .withColumn("dailySold", col("daily.sold") - col("state.sold"))
            .withColumn(
                "gmv",
                col("dailySold") * col("daily.sellPrice")
            )
            .withColumn("createdDate", now)
            .select(
                col("daily.id"),
                col("daily.price"),
                col("daily.sellPrice"),
                col("createdDate"),
                col("dailySold"),
                col("gmv")
            )
        )
        result_df.writeTo(PRODUCT_ITEM_DAILY_REVENUE_TABLE).overwritePartitions()

