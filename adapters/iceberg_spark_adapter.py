from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.functions import to_date, col
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
        print(iceberg_config)
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

    def traverse_table(self, table_name: str) -> DataFrame:
        return self.spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC;")

    def rollback_table(self, table_name: str, timestamp) -> str:
        return  f"CALL system.rollback_to_timestamp({table_name},TIMESTAMP '{timestamp})"

    def get_table_by_version(self, table_name: str, snapshot_id: str) -> DataFrame:
        return self.spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {snapshot_id}")

    def get_latest_snapshot_by_date(self, table_name: str, process_date: str) -> DataFrame:
        df = self.traverse_table(table_name)
        result = (
            df.filter(to_date(col("committed_at")) == process_date)
            .orderBy(col("committed_at").desc())
            .select("snapshot_id")
            .limit(1)
            .collect()
        )
        if not result:
            raise ValueError(f"No snapshot found for date {process_date}")
        return result[0]["snapshot_id"]

iceberg_spark_adapter = IcebergSparkAdapter()
