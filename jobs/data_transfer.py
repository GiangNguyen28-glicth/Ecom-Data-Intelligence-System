import json
import sys
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, to_date

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from helpers.helpers import Helper
from schema.report import parsed_product_item_schema


class DataTransfer:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def transfer_to_iceberg_pid(self, config):
        parsed_bucket = config["parsed_bucket"]
        from_date= config["from_date"]
        to_date= config["from_date"]
        from_year, from_month, from_day = [int(x) for x in from_date.split("-")]
        to_year, to_month, to_day = [int(x) for x in to_date.split("-")]

        daily_table = config["daily_table"]
        bucket = Helper.get_bucket(parsed_bucket)
        df = self.spark.read.schema(parsed_product_item_schema).parquet(f"{bucket}")
        df = df.filter(
            (col("year") > from_year) |
            ((col("year") == from_year) & (col("month") > from_month)) |
            ((col("year") == from_year) & (col("month") == from_month) & (col("day") >= from_day))
        ).filter(
            (col("year") < to_year) |
            ((col("year") == to_year) & (col("month") < to_month)) |
            ((col("year") == to_year) & (col("month") == to_month) & (col("day") <= to_day))
        )

        df = df.filter(col("sold") > 0).select("id", "sold", "price", "sellPrice", "crawledDateMs")
        window = Window.partitionBy("id") \
            .orderBy(col("crawledDateMs").desc())
        cleaned_df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
        cleaned_df.writeTo(daily_table).overwritePartitions()

    def transfer_to_iceberg_latest_pi(self, config):
        process_date = config["process_date"]
        daily_table = config["daily_table"]
        state_table = config["state_table"]
        df = self.spark.read.table(daily_table)
        df = df.filter(
            to_date(col("crawledDateMs"))
            == process_date
        ).filter(col("sold").isNotNull()).select("id", "sold", "crawledDateMs")

        window = Window.partitionBy("id") \
            .orderBy(col("crawledDateMs").desc())
        cleaned_df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
        cleaned_df.createOrReplaceTempView("tmp_latest_pi")
        self.spark.sql(f"""
                MERGE INTO {state_table} t
                USING tmp_latest_pi s
                ON t.id = s.id
                WHEN MATCHED THEN
                  UPDATE SET
                    t.sold = s.sold,
                    t.crawledDateMs = s.crawledDateMs
                WHEN NOT MATCHED THEN
                  INSERT (id, sold, crawledDateMs)
                  VALUES (s.id, s.sold, s.crawledDateMs)
            """)

    def check_data_ready(self, config):
        process_date = config["process_date"]
        year, month, day = [int(x) for x in process_date.split("-")]
        bucket = Helper.get_bucket(config["parsed_bucket"])
        df = self.spark.read.parquet(bucket) \
            .filter((col("year") == year) & (col("month") == month) & (col("day") == day))

        count = df.limit(1).count()

        if count > 0:
            print(f"Data found for {process_date}")
            return True
        else:
            print(f"No data yet for {process_date}")
            return False


if __name__ == "__main__":
    job = DataTransfer()

    mode = sys.argv[1]
    config = json.loads(sys.argv[2])
    print("Mode: ",mode)
    print("Config: ",config)
    if mode == "transfer_to_iceberg_pid":
        job.transfer_to_iceberg_pid(config)
    elif mode == "transfer_to_iceberg_latest_pi":
        job.transfer_to_iceberg_latest_pi(config)
    elif mode == "check_data_ready":
        job.check_data_ready(config)
    else:
        raise ValueError("Invalid mode")
