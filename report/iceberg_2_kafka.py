from pyspark.sql.functions import col, to_json, struct, monotonically_increasing_id, floor, collect_list

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import PRODUCT_ITEM_DAILY_REVENUE_TABLE
from helpers.helpers import Helper


class Iceberg2Kafka:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def load_pid_revenue(self):
        batch = 10
        current_date = Helper.get_current_date()
        # process_date = f"{current_date['year']}-{current_date['month']}-{current_date['day']}"
        process_date = f"{current_date['year']}-{current_date['month']}-17"
        print("Process date:", process_date)
        df = self.spark.read.table(PRODUCT_ITEM_DAILY_REVENUE_TABLE).filter(col("createdDate") >= process_date)
        df = df.withColumn("_row_id", monotonically_increasing_id())
        df = df.withColumn("_group_id", floor(col("_row_id") / batch))
        df = df.withColumn("packed_row", struct("*"))
        df_batch = df.groupBy("_group_id") \
            .agg(collect_list("packed_row").alias("data_array"))
        df_kafka = df_batch.select(to_json(col("data_array")).alias("value"))
        df_kafka.write.format("kafka") \
            .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
            .option("topic", "clickhouse_ingest_topic") \
            .save()
        print("Batch Job Completed Successfully!")