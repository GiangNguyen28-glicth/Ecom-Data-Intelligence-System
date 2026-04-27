import json
import sys

from pyspark.sql.functions import col, broadcast, current_timestamp, to_timestamp, expr, lit
from adapters.iceberg_spark_adapter import iceberg_spark_adapter


class CalcRevenue:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def calc_daily_sold(self, config):
        process_date = config["process_date"]
        state_table = config["state_table"]
        daily_table = config["daily_table"]
        mart_daily_revenue_table = config["mart_daily_revenue_table"]
        print("Config:", config)
        daily_df = (
            self.spark.read.table(daily_table)
            .filter(
                (col("crawledDateMs") >= to_timestamp(lit(process_date))) &
                (col("crawledDateMs") < expr(f"timestamp('{process_date}') + INTERVAL 1 DAY"))
            )
        )
        state_df = self.spark.read.table(state_table)
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
            .withColumn("createdDate", current_timestamp())
            .select(
                col("daily.id"),
                col("daily.price"),
                col("daily.sellPrice"),
                col("createdDate"),
                col("dailySold"),
                col("gmv"),
                col("daily.crawledDateMs")
            )
        )
        result_df.writeTo(mart_daily_revenue_table).overwritePartitions()


if __name__ == "__main__":
    job = CalcRevenue()
    mode = sys.argv[1]
    config = json.loads(sys.argv[2])
    if mode == "calc_daily_sold":
        job.calc_daily_sold(config)
    else:
        raise ValueError("Invalid mode")
