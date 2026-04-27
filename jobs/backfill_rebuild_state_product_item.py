import json
import sys

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import LAST_STATE_PRODUCT_ITEM_STAGING_TABLE, LAST_STATE_PRODUCT_ITEM_TABLE


class RebuildProductLatestState:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def create_state_product_item_staging(self, config):
        from_date: str = config["from_date"]
        state_product_item_staging_table: str = config["state_product_item_staging_table"]
        snapshot_id = iceberg_spark_adapter.get_latest_snapshot_by_date(LAST_STATE_PRODUCT_ITEM_TABLE, from_date)
        print(f"Roll back at snapshot id: {snapshot_id}")
        df = iceberg_spark_adapter.get_table_by_version(LAST_STATE_PRODUCT_ITEM_TABLE, snapshot_id)
        df.writeTo(state_product_item_staging_table).createOrReplace()

    # def build_state_product_item_staging(self, config):
    #
    #     paths = Helper.get_paths_by_minio_format_n_time_range(process_date, process_date)
    #     df = self.spark.read.parquet(*paths)


if __name__ == "__main__":
    job = RebuildProductLatestState()

    mode = sys.argv[1]
    config = json.loads(sys.argv[2])

    if mode == "create_state_product_item_staging":
        job.create_state_product_item_staging(config)
    else:
        raise ValueError("Invalid mode")
