from pyspark.sql import SparkSession

class SparkAdapter:
    def __init__(self, spark_config):
        print('Initializing Spark Adapter')
        self.spark_config = spark_config

    def create_spark_session(self):
        print('Initializing Spark Session')
        builder = SparkSession.builder.appName(self.spark_config.app_name).master(self.spark_config.master)
        spark_session = builder.getOrCreate()
        return spark_session
