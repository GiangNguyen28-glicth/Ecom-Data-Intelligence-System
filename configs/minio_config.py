from typing import Dict

from configs.settings import settings


class MinioConfig:
    def __init__(
            self,
            endpoint: str = settings.minio_endpoint,
            access_key: str = settings.minio_access_key,
            secret_key: str = settings.minio_secret_key,
            path_style_access: bool = True,
            ssl_enabled: bool = False,
            **additional_configs: Dict[str, str]
    ):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.path_style_access = path_style_access
        self.ssl_enabled = ssl_enabled
        self.additional_configs = additional_configs

    def to_spark_configs(self) -> Dict[str, str]:
        configs = {
            "spark.hadoop.fs.s3a.endpoint": self.endpoint,
            "spark.hadoop.fs.s3a.access.key": self.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.secret_key,
            "spark.hadoop.fs.s3a.path.style.access": str(self.path_style_access).lower(),
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(self.ssl_enabled).lower(),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.sql.hive.manageFilesourcePartitions": "true",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.hadoop.fs.s3a.connection.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "60000"
        }
        configs.update(self.additional_configs)
        return configs
