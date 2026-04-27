import json
import sys

from pyspark.sql.functions import col, to_json, struct, monotonically_increasing_id, floor, collect_list, to_date

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import PRODUCT_ITEM_DAILY_REVENUE_TABLE
from configs.settings import settings
from helpers.helpers import Helper


class Iceberg2Kafka:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def pid_revenue_2_kafka(self, config):
        batch = config["batch"]
        process_date = config['process_date']
        mart_daily_revenue_table = config["mart_daily_revenue_table"]
        print("Process date:", process_date)
        df = self.spark.read.table(mart_daily_revenue_table).filter(to_date("createdDate") == process_date)
        df = df.withColumn("_row_id", monotonically_increasing_id())
        df = df.withColumn("_group_id", floor(col("_row_id") / batch))
        df = df.withColumn("packed_row", struct("*"))
        df_batch = df.groupBy("_group_id") \
            .agg(collect_list("packed_row").alias("data_array"))
        df_kafka = df_batch.select(to_json(col("data_array")).alias("value"))
        df_kafka.write.format("kafka") \
            .option("kafka.bootstrap.servers", settings.kafka_host) \
            .option("topic", "clickhouse_ingest_topic") \
            .save()
        print("Batch Job Completed Successfully!")


if __name__ == "__main__":
    job = Iceberg2Kafka()

    mode = sys.argv[1]
    config = json.loads(sys.argv[2])
    if mode == "pid_revenue_2_kafka":
        job.pid_revenue_2_kafka(config)
    else:
        raise ValueError("Invalid mode")