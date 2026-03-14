from pyspark.sql.functions import split, count, regexp_extract

from adapters.minio_spark_adapter import minio_spark_adapter


class NasaHttpLog:
    def __init__(self):
        self.spark = minio_spark_adapter.create_spark_session()

    def create_df_http_log(self):
        return self.spark.read.text(f"s3a://kaggle-data/nasa_http.log")

    def count_total_log(self):
        df = self.create_df_http_log()
        df.show(2)

    def top_10_ip_access(self):
        df = self.create_df_http_log()
        df = df.withColumn("ip_access", split(df.value, " ")[0])
        df.groupby(df.ip_access).agg(count("*").alias("total_access")).orderBy("total_access", ascending=False).show(10)

    def statistics_log(self):
        df = self.create_df_http_log()
        df = df.withColumn("status_code", regexp_extract("value", r'"[^"]*"\s(\d{3})', 1).cast("int"))
        # df.groupby("status_code").agg(count("*").alias("total_count")).show(10)
        df.cache()
        total_requests = df.count()
        total_error_requests = df.filter(df.status_code >= 400).explain(True).count()
        error_rate = total_error_requests / total_requests * 100
        print(f"Error rate: {error_rate:.2f}%")
