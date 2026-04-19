from pyspark.sql.types import StructType, LongType, DoubleType, FloatType, StructField, StringType

from adapters.iceberg_spark_adapter import iceberg_spark_adapter


class Iceberg:
    def __init__(self):
        print('Initializing Iceberg')
        self.spark = iceberg_spark_adapter.create_spark_session()

    def created_database(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS ecm_catalog.nyc")

    def created_table(self):
        schema = StructType([
            StructField("vendor_id", LongType(), True),
            StructField("trip_id", LongType(), True),
            StructField("trip_distance", FloatType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True)
        ])
        df = self.spark.createDataFrame([], schema)
        df.writeTo("ecm_catalog.nyc.taxis").create()

    def write_demo_data(self):
        schema = self.spark.table("ecm_catalog.nyc.taxis").schema
        data = [
            (1, 1000371, 1.8, 15.32, "N"),
            (2, 1000372, 2.5, 22.15, "N"),
            (2, 1000373, 0.9, 9.01, "N"),
            (1, 1000374, 8.4, 42.13, "Y")
        ]
        df = self.spark.createDataFrame(data, schema)
        df.writeTo("ecm_catalog.nyc.taxis").append()

    def show_data (self):
        df = self.spark.table("ecm_catalog.nyc.taxis").show()