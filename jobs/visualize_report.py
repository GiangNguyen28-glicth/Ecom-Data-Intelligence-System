from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from helpers.helpers import Helper

class OpensanctionsVisualizeWeight:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def process(self, config):
        parsed_bucket = config["bucket"]
        bucket = Helper.get_bucket(parsed_bucket)

        input_path = f"{bucket}/entities.ftm.json"
        output_path = f"{bucket}/validation/weight_formats"

        raw_df = self.spark.read.json(input_path)

        print("Input path:", input_path)
        print("Output path:", output_path)

        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

        weight_df = self.prepare_weight_data(raw_df)

        print("Total weight values:", weight_df.count())

        format_summary_df = self.build_format_summary(weight_df)
        dataset_summary_df = self.build_dataset_summary(weight_df)
        raw_value_summary_df = self.build_raw_value_summary(weight_df)
        entity_summary_df = self.build_entity_summary(raw_df)
        ambiguous_df = self.build_ambiguous_weight_report(weight_df)

        print("\n===== WEIGHT FORMAT SUMMARY =====")
        format_summary_df.show(100, truncate=False)

        print("\n===== WEIGHT FORMAT BY DATASET =====")
        dataset_summary_df.show(200, truncate=False)

        print("\n===== MOST COMMON RAW VALUES =====")
        raw_value_summary_df.show(200, truncate=False)

        print("\n===== ENTITIES WITH MULTIPLE WEIGHTS OR DATASETS =====")
        entity_summary_df.show(100, truncate=False)

        print("\n===== AMBIGUOUS NUMERIC-ONLY WEIGHTS =====")
        ambiguous_df.show(200, truncate=False)

        self.write_report(
            format_summary_df,
            f"{output_path}/format_summary"
        )

        self.write_report(
            dataset_summary_df,
            f"{output_path}/dataset_summary"
        )

        self.write_report(
            raw_value_summary_df,
            f"{output_path}/raw_value_summary"
        )

        self.write_report(
            entity_summary_df,
            f"{output_path}/entity_summary"
        )

        self.write_report(
            ambiguous_df,
            f"{output_path}/ambiguous_numeric_weights"
        )

    def prepare_weight_data(self, raw_df):
        """
        Chuẩn hóa properties.weight thành từng row riêng.

        Output chính:
        - entity_id
        - datasets
        - dataset_count
        - weight_count
        - weight_raw
        - weight_normalized
        - weight_format
        - numeric_value
        """

        properties_type = raw_df.schema["properties"].dataType
        weight_field = next(
            (
                field
                for field in properties_type.fields
                if field.name == "weight"
            ),
            None
        )

        if weight_field is None:
            raise ValueError(
                "Field properties.weight does not exist in input schema"
            )

        if isinstance(weight_field.dataType, ArrayType):
            weight_array = F.col("properties.weight")
        else:
            weight_array = F.when(
                F.col("properties.weight").isNull(),
                F.array()
            ).otherwise(
                F.array(F.col("properties.weight"))
            )

        base_df = (
            raw_df
            .select(
                F.col("id").alias("entity_id"),
                F.col("schema").alias("entity_schema"),
                F.col("caption"),
                F.coalesce(
                    F.col("datasets"),
                    F.array().cast("array<string>")
                ).alias("datasets"),
                weight_array.alias("weights")
            )
            .filter(F.size(F.col("weights")) > 0)
            .withColumn(
                "dataset_count",
                F.size(F.col("datasets"))
            )
            .withColumn(
                "weight_count",
                F.size(F.col("weights"))
            )
        )

        exploded_df = (
            base_df
            .withColumn(
                "weight_raw",
                F.explode_outer(F.col("weights"))
            )
            .filter(F.col("weight_raw").isNotNull())
            .withColumn(
                "weight_normalized",
                F.lower(
                    F.trim(
                        F.col("weight_raw").cast("string")
                    )
                )
            )
        )

        exploded_df = exploded_df.withColumn(
            "weight_format",
            self.classify_weight_format(
                F.col("weight_normalized")
            )
        )

        numeric_text = F.regexp_extract(
            F.col("weight_normalized"),
            r"([0-9]+(?:\.[0-9]+)?)",
            1
        )

        exploded_df = exploded_df.withColumn(
            "numeric_value",
            F.when(
                numeric_text != "",
                numeric_text.cast("double")
            ).otherwise(
                F.lit(None).cast("double")
            )
        )

        exploded_df = exploded_df.withColumn(
            "all_numeric_values",
            F.expr(
                """
                transform(
                    regexp_extract_all(
                        weight_normalized,
                        '([0-9]+(?:\\\\.[0-9]+)?)',
                        1
                    ),
                    x -> cast(x as double)
                )
                """
            )
        )

        return exploded_df

    def classify_weight_format(self, weight_col):
        """
        Phân loại format weight.

        Thứ tự điều kiện rất quan trọng:
        format cụ thể phải được kiểm tra trước format tổng quát.
        """

        return (
            F.when(
                weight_col.isNull() | (weight_col == ""),
                F.lit("empty")
            )

            # Ví dụ:
            # 180 to 210 pounds
            # 180-210 lb
            .when(
                weight_col.rlike(
                    r"^\s*[0-9]+(?:\.[0-9]+)?\s*"
                    r"(?:to|-)\s*"
                    r"[0-9]+(?:\.[0-9]+)?\s*"
                    r"(?:lb|lbs|pound|pounds)\s*$"
                ),
                F.lit("pound_range")
            )

            # Ví dụ:
            # 80 to 90 kg
            # 80-90 kilograms
            .when(
                weight_col.rlike(
                    r"^\s*[0-9]+(?:\.[0-9]+)?\s*"
                    r"(?:to|-)\s*"
                    r"[0-9]+(?:\.[0-9]+)?\s*"
                    r"(?:kg|kgs|kilogram|kilograms)\s*$"
                ),
                F.lit("kilogram_range")
            )

            # Ví dụ:
            # 132 pounds (60 kg)
            # 132 lb / 60 kg
            .when(
                weight_col.rlike(
                    r".*(?:lb|lbs|pound|pounds).*"
                    r"(?:kg|kgs|kilogram|kilograms).*"
                )
                |
                weight_col.rlike(
                    r".*(?:kg|kgs|kilogram|kilograms).*"
                    r"(?:lb|lbs|pound|pounds).*"
                ),
                F.lit("mixed_pound_kilogram")
            )

            # Ví dụ:
            # 135 pounds
            # 135 lb
            .when(
                weight_col.rlike(
                    r"^\s*[0-9]+(?:\.[0-9]+)?\s*"
                    r"(?:lb|lbs|pound|pounds)\s*$"
                ),
                F.lit("pound")
            )

            # Ví dụ:
            # 61 kg
            # 61 kilograms
            .when(
                weight_col.rlike(
                    r"^\s*[0-9]+(?:\.[0-9]+)?\s*"
                    r"(?:kg|kgs|kilogram|kilograms)\s*$"
                ),
                F.lit("kilogram")
            )

            # Ví dụ:
            # 135
            # 200.5
            .when(
                weight_col.rlike(
                    r"^\s*[0-9]+(?:\.[0-9]+)?\s*$"
                ),
                F.lit("numeric_only")
            )

            # Có chữ pound nhưng format không chuẩn
            .when(
                weight_col.rlike(
                    r".*\b(?:lb|lbs|pound|pounds)\b.*"
                ),
                F.lit("pound_unstructured")
            )

            # Có chữ kg nhưng format không chuẩn
            .when(
                weight_col.rlike(
                    r".*\b(?:kg|kgs|kilogram|kilograms)\b.*"
                ),
                F.lit("kilogram_unstructured")
            )

            .otherwise(
                F.lit("unknown")
            )
        )

    def build_format_summary(self, weight_df):
        """
        Tổng số weight theo format.
        """

        return (
            weight_df
            .groupBy("weight_format")
            .agg(
                F.count("*").alias("weight_value_count"),
                F.countDistinct("entity_id").alias("entity_count"),
                F.countDistinct("weight_raw").alias(
                    "distinct_raw_value_count"
                ),
                F.min("numeric_value").alias("min_numeric_value"),
                F.max("numeric_value").alias("max_numeric_value"),
                F.avg("numeric_value").alias("avg_numeric_value")
            )
            .orderBy(
                F.desc("weight_value_count")
            )
        )

    def build_dataset_summary(self, weight_df):
        """
        Explode datasets để biết mỗi dataset đang có format nào.

        Lưu ý:
        Dataset và weight không có positional relationship.
        Report này chỉ cho biết entity chứa dataset đó có weight format nào,
        không khẳng định chính dataset đó cung cấp từng weight value.
        """

        return (
            weight_df
            .withColumn(
                "dataset",
                F.explode_outer(F.col("datasets"))
            )
            .groupBy(
                "dataset",
                "weight_format"
            )
            .agg(
                F.count("*").alias("weight_value_count"),
                F.countDistinct("entity_id").alias("entity_count"),
                F.countDistinct("weight_raw").alias(
                    "distinct_raw_value_count"
                ),
                F.min("numeric_value").alias("min_numeric_value"),
                F.max("numeric_value").alias("max_numeric_value"),
                F.avg("numeric_value").alias("avg_numeric_value")
            )
            .orderBy(
                "dataset",
                F.desc("weight_value_count")
            )
        )

    def build_raw_value_summary(self, weight_df):
        """
        Thống kê raw value phổ biến.

        Giúp phát hiện:
        - 135
        - 150
        - 200
        - 315
        - 90 kg kg
        - 180 to 210 pounds
        """

        return (
            weight_df
            .groupBy(
                "weight_format",
                "weight_raw"
            )
            .agg(
                F.count("*").alias("occurrence_count"),
                F.countDistinct("entity_id").alias("entity_count"),
                F.collect_set("datasets").alias(
                    "dataset_combinations"
                )
            )
            .orderBy(
                F.desc("occurrence_count"),
                "weight_format",
                "weight_raw"
            )
        )

    def build_entity_summary(self, raw_df):
        """
        Tìm entity có:
        - nhiều weight values
        - nhiều datasets
        - hoặc cả hai

        Đây là nhóm cần kiểm tra vì không thể map weight với dataset
        theo vị trí array.
        """

        return (
            raw_df
            .select(
                F.col("id").alias("entity_id"),
                F.col("schema").alias("entity_schema"),
                F.col("caption"),
                F.coalesce(
                    F.col("datasets"),
                    F.array().cast("array<string>")
                ).alias("datasets"),
                F.coalesce(
                    F.col("properties.weight"),
                    F.array().cast("array<string>")
                ).alias("weights")
            )
            .withColumn(
                "dataset_count",
                F.size(F.col("datasets"))
            )
            .withColumn(
                "weight_count",
                F.size(F.col("weights"))
            )
            .filter(
                (F.col("dataset_count") > 1)
                | (F.col("weight_count") > 1)
            )
            .orderBy(
                F.desc("weight_count"),
                F.desc("dataset_count")
            )
        )

    def build_ambiguous_weight_report(self, weight_df):
        """
        Numeric-only weight là nhóm chưa có unit.

        Phân loại mức ambiguity:
        - single_dataset:
          Có thể suy luận theo source nếu source có rule rõ ràng.
        - multiple_datasets:
          Không thể xác định weight đến từ dataset nào.
        """

        return (
            weight_df
            .filter(
                F.col("weight_format") == "numeric_only"
            )
            .withColumn(
                "ambiguity_type",
                F.when(
                    F.col("dataset_count") == 1,
                    F.lit("single_dataset")
                ).otherwise(
                    F.lit("multiple_datasets")
                )
            )
            .select(
                "entity_id",
                "entity_schema",
                "caption",
                "datasets",
                "dataset_count",
                "weight_count",
                "weight_raw",
                "numeric_value",
                "ambiguity_type"
            )
            .orderBy(
                F.desc("dataset_count"),
                "datasets",
                "numeric_value"
            )
        )

    def write_report(self, df, output_path):
        """
        Parquet phù hợp hơn JSON cho report dữ liệu lớn.

        Có thể đổi thành JSON nếu cần đọc thủ công:
        .format("json")
        """

        (
            df
            .coalesce(1)
            .write
            .mode("overwrite")
            .format("parquet")
            .save(output_path)
        )