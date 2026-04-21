from pyspark.sql import SparkSession

from configs.minio_config import MinioConfig


class MinioSparkAdapter:
    def __init__(self, minio_config: MinioConfig):
        self.minio_config = minio_config
    def create_config(self):
        builder = SparkSession.builder.appName("Minio Spark").master("local[*]")
        for key, value in self.minio_config.to_spark_configs().items():
            print(key, value)
            builder = builder.config(key, value)
        return builder

    def create_spark_session(self) -> SparkSession:
        return self.create_config().getOrCreate()

minio_spark_adapter = MinioSparkAdapter(MinioConfig())
