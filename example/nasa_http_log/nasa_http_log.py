from pyspark.sql.functions import split, count, regexp_extract, sum, col, expr, avg, hour, to_timestamp
from adapters.minio_spark_adapter import minio_spark_adapter


class NasaHttpLog:
    def __init__(self):
        self.spark = minio_spark_adapter.create_spark_session()

    def create_df_http_log(self):
        return self.spark.read.text(f"s3a://kaggle-data/nasa_http.log")

    def parse_log(self):
        pattern = r'^(\S+) - - \[(.*?)\] "(\S+) (\S+) .*?" (\d{3}) (\S+)'
        df = self.create_df_http_log()
        df = df.select(
            regexp_extract("value", pattern, 1).alias("ip"),
            regexp_extract("value", pattern, 2).alias("timestamp"),
            regexp_extract("value", pattern, 3).alias("method"),
            regexp_extract("value", pattern, 4).alias("endpoint"),
            regexp_extract("value", pattern, 5).alias("status"),
            regexp_extract("value", pattern, 6).alias("bytes"),
        )
        df = df.withColumn("status", expr("try_cast(status as int)")).withColumn("bytes",
                                                                                 expr(
                                                                                     "try_cast(bytes as int)")).withColumn(
            "timestamp", expr("try_to_timestamp(timestamp, 'dd/MMM/yyyy:HH:mm:ss Z')"))
        return df

    def count_total_log(self):
        df = self.create_df_http_log()
        df.show(10)

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
        total_error_requests = df.filter(df.status_code >= 400).count()
        error_rate = total_error_requests / total_requests * 100
        print(f"Error rate: {error_rate:.2f}%")

    def statistics_endpoint(self):
        df = self.parse_log()
        df = df.groupby("endpoint").agg(count("*").alias("total_endpoint")).orderBy("total_endpoint", ascending=False)
        df.show(10)

    def statistic_traffic_bytes(self):
        df = self.parse_log()
        df = df.filter(col("bytes").isNotNull())
        df.cache()
        df.select(sum("bytes").alias("total_traffic_bytes"), avg("bytes").alias("avg_traffic_bytes")).show()

    def statistic_traffic_errors(self):
        df = self.parse_log()
        df.filter(col("status") >= 400) \
            .groupby("endpoint") \
            .agg(count("*").alias("total_traffic_errors")) \
            .orderBy(
            "total_traffic_errors", ascending=False) \
            .show()

    def statistic_peak_traffic_by_hour(self):
        df = self.parse_log()
        df = df.withColumn("hour", hour(col("timestamp")))
        df.groupby("hour").agg(count("*").alias("total_traffic")).orderBy("hour", ascending=False).show()

