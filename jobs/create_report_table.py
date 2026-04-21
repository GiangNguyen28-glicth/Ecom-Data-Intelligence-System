from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from migrations.report import state_pi_ddl, pid_ddl, pid_revenue_ddl
from schema.report import last_state_product_item_schema, product_item_daily_schema


class CreateReportTable:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def load_df(self):
        dataset_path = f"s3a://parsed-data/2026/04/15"
        # return self.spark.read.parquet(dataset_path)
        return self.spark.read.schema(product_item_daily_schema).parquet(dataset_path)

    def create_database(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS ecm_catalog.report")

    def create_state_table(self):
        self.spark.sql(state_pi_ddl)

    def create_pid_table(self):
        self.spark.sql(pid_ddl)

    def create_revenue_table(self):
        self.spark.sql(pid_revenue_ddl)

    def main(self):
        self.create_database()
        self.create_state_table()