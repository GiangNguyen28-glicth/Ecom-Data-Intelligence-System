from pyspark.sql.functions import col, year, month, dayofmonth
from adapters.minio_spark_adapter import minio_spark_adapter


class Unknown:
    def __init__(self):
        print('Initializing ECMOlist')
        self.spark = minio_spark_adapter.create_spark_session()

    def load_df(self, dataset_name: str):
        dataset_path = f"s3a://kaggle-data/{dataset_name}.csv"
        return self.spark.read.csv(dataset_path, header=True, inferSchema=True)

    def extract_event_timestamp(self):
        df = self.load_df('de_assessment_data')
        df = df.withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
        df = df \
        .withColumn("year", year(col("event_timestamp"))) \
        .withColumn("month", month(col("event_timestamp"))) \
        .withColumn("day", dayofmonth(col("event_timestamp")))
        df.select("year", "month", "day").distinct().show(1000)
