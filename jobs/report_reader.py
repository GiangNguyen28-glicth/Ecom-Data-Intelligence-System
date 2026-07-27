from pyspark.sql import functions as F
from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from helpers.helpers import Helper

class OpensanctionsReportReader:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def process(self, config):
        parsed_bucket = config["bucket"]
        bucket = Helper.get_bucket(parsed_bucket)

        report_base_path = f"{bucket}/validation/weight_formats"

        format_summary_df = self.read_report(
            f"{report_base_path}/format_summary"
        )

        dataset_summary_df = self.read_report(
            f"{report_base_path}/dataset_summary"
        )

        raw_value_summary_df = self.read_report(
            f"{report_base_path}/raw_value_summary"
        )

        entity_summary_df = self.read_report(
            f"{report_base_path}/entity_summary"
        )

        ambiguous_df = self.read_report(
            f"{report_base_path}/ambiguous_numeric_weights"
        )

        # self.show_format_summary(format_summary_df)
        # self.show_dataset_summary(dataset_summary_df)
        # self.show_raw_values(raw_value_summary_df)
        # self.show_multi_source_entities(entity_summary_df)
        self.show_ambiguous_weights(ambiguous_df)

    def read_report(self, path):
        print(f"\nReading report: {path}")

        return (
            self.spark.read
            .format("parquet")
            .load(path)
        )

    def show_format_summary(self, df):
        print("\n===== 1. WEIGHT FORMAT SUMMARY =====")

        (
            df
            .orderBy(F.desc("weight_value_count"))
            .show(100, truncate=False)
        )

    def show_dataset_summary(self, df):
        print("\n===== 2. FORMAT BY DATASET =====")

        (
            df
            .orderBy(
                "dataset",
                F.desc("weight_value_count")
            )
            .show(500, truncate=False)
        )

    def show_raw_values(self, df):
        print("\n===== 3. MOST COMMON RAW WEIGHT VALUES =====")

        (
            df
            .orderBy(F.desc("occurrence_count"))
            .show(500, truncate=False)
        )

    def show_multi_source_entities(self, df):
        print("\n===== 4. MULTIPLE WEIGHTS / MULTIPLE DATASETS =====")

        (
            df
            .orderBy(
                F.desc("dataset_count"),
                F.desc("weight_count")
            )
            .show(500, truncate=False)
        )

    def show_ambiguous_weights(self, df):
        print("\n===== 5. AMBIGUOUS NUMERIC-ONLY WEIGHTS =====")

        (
            df
            .orderBy(
                F.desc("dataset_count"),
                "numeric_value"
            )
            .show(500, truncate=False)
        )