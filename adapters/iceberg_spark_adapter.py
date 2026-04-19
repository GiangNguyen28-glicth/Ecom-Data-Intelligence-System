from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

from adapters.minio_spark_adapter import MinioSparkAdapter
from configs.minio_config import MinioConfig
from configs.settings import settings


class IcebergSparkAdapter:
    def __init__(self):
        self.spark = self._create_spark_session()

    def _init_iceberg_config(self) -> MinioConfig:
        iceberg_config = {
            "spark.sql.defaultCatalog": "ecm_catalog",
            "spark.sql.catalog.ecm_catalog.type": "hadoop",
            "spark.sql.catalog.ecm_catalog.uri": settings.minio_endpoint,
            "spark.sql.catalog.ecm_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.ecm_catalog.warehouse": "s3a://warehouse/",
            "spark.sql.catalog.ecm_catalog.s3.endpoint": settings.minio_endpoint,
            "spark.sql.catalog.ecm_catalog.s3.access-key-id": settings.minio_access_key,
            "spark.sql.catalog.ecm_catalog.s3.secret-access-key": settings.minio_secret_key,
            "spark.sql.catalog.ecm_catalog.s3.region": "us-east-1",
            "spark.sql.catalog.ecm_catalog.s3.path-style-access": "true",
            "spark.sql.catalog.ecm_catalog.client.region": "us-east-1",
            "spark.sql.catalog.ecm_catalog": "org.apache.iceberg.spark.SparkCatalog",
            # "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        }
        minio_config = MinioConfig(**iceberg_config)
        return minio_config

    def _create_spark_session(self) -> SparkSession:
        iceberg_config = self._init_iceberg_config()
        minio_spark_adapter = MinioSparkAdapter(iceberg_config)
        spark_session = minio_spark_adapter.create_spark_session()
        return spark_session

    def create_database(self, database_name: str):
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    def create_table(self, table_name: str, table_schema: StructType):
        df = self.spark.createDataFrame([], table_schema)
        df.writeTo(table_name).create()

    def drop_table(self, table_name: str):
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def truncate_table(self, table_name: str):
        self.spark.sql(f"TRUNCATE TABLE {table_name}")

iceberg_spark_adapter = IcebergSparkAdapter()
