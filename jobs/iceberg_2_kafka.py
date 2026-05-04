import json
import sys

from pyspark.sql.functions import col, to_json, struct, monotonically_increasing_id, floor, collect_list, to_date, lit, \
    date_add

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from configs.settings import settings


class Iceberg2Kafka:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def pid_revenue_2_kafka(self, config):
        batch = config["batch"]
        from_date = config['from_date']
        to_date_val = config['to_date']
        output_topic = config['output_topic']
        mart_daily_revenue_table = config["mart_daily_revenue_table"]
        print("Config:", config)
        df = self.spark.read.table(mart_daily_revenue_table) .filter(
            (col("crawledDateMs") >= to_date(lit(from_date))) &
            (col("crawledDateMs") < date_add(to_date(lit(to_date_val)), 1))
        )
        df = df.withColumn("_row_id", monotonically_increasing_id())
        df = df.withColumn("_group_id", floor(col("_row_id") / batch))
        df = df.withColumn("packed_row", struct("*"))
        df_batch = df.groupBy("_group_id") \
            .agg(collect_list("packed_row").alias("data_array"))
        df_kafka = df_batch.select(to_json(col("data_array")).alias("value"))
        df_kafka.write.format("kafka") \
            .option("kafka.bootstrap.servers", settings.kafka_host) \
            .option("topic", output_topic) \
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