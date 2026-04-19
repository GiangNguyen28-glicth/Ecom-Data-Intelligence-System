from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import PARSED_BUCKET, PRODUCT_ITEM_DAILY_TABLE, LAST_STATE_PRODUCT_ITEM_TABLE
from helpers.helpers import Helper


class DataTransfer:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def transfer_to_iceberg_pid(self):
        current_date = Helper.get_current_date()
        bucket = Helper.get_bucket(PARSED_BUCKET)
        df = self.spark.read.parquet(f"{bucket}/{current_date['year']}/{current_date['month']}/{current_date['day']}")
        df = df.filter(col("sold") > 0).select("id", "sold", "price", "sellPrice", "crawledDateMs")
        window = Window.partitionBy("id") \
            .orderBy(col("crawledDateMs").desc())
        cleaned_df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
        cleaned_df.writeTo(PRODUCT_ITEM_DAILY_TABLE).overwritePartitions()

    def transfer_to_iceberg_latest_pi(self):
        current_date = Helper.get_current_date()
        bucket = Helper.get_bucket(PARSED_BUCKET)
        df = self.spark.read.parquet(f"{bucket}/{current_date['year']}/{current_date['month']}/{current_date['day']}")
        df = df.filter(col("sold").isNotNull()).select("id", "sold", "crawledDateMs")
        window = Window.partitionBy("id") \
            .orderBy(col("crawledDateMs").desc())
        cleaned_df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
        cleaned_df.createOrReplaceTempView("tmp_latest_pi")
        self.spark.sql(f"""
                MERGE INTO {LAST_STATE_PRODUCT_ITEM_TABLE} t
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