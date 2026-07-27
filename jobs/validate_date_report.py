from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from helpers.helpers import Helper
class OpensanctionsBirthDateReport:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def process(self, config):
        parsed_bucket = config["bucket"]
        bucket = Helper.get_bucket(parsed_bucket)

        input_path = f"{bucket}/entities.ftm.json"
        output_path = f"{bucket}/validation/birth_date_formats"

        raw_df = self.spark.read.json(input_path)

        birth_date_df = self.prepare_birth_date_data(raw_df)
        format_summary_df = self.build_format_summary(birth_date_df)
        invalid_summary_df = self.build_invalid_summary(birth_date_df)
        sample_df = self.build_format_samples(birth_date_df)

        print("\n===== BIRTH DATE FORMAT SUMMARY =====")
        format_summary_df.show(100, truncate=False)

        print("\n===== INVALID BIRTH DATE VALUES =====")
        invalid_summary_df.show(200, truncate=False)

        print("\n===== SAMPLE BY FORMAT =====")
        sample_df.show(500, truncate=False)

        self.write_report(
            format_summary_df,
            f"{output_path}/format_summary"
        )

        self.write_report(
            invalid_summary_df,
            f"{output_path}/invalid_summary"
        )

        self.write_report(
            sample_df,
            f"{output_path}/format_samples"
        )

    def prepare_birth_date_data(self, raw_df):
        birth_date_field = self.get_birth_date_field(raw_df)

        if birth_date_field is None:
            raise ValueError(
                "Field properties.birthDate does not exist in input schema"
            )

        if isinstance(birth_date_field.dataType, ArrayType):
            birth_date_array = F.col("properties.birthDate")
        else:
            birth_date_array = (
                F.when(
                    F.col("properties.birthDate").isNull(),
                    F.array().cast("array<string>")
                )
                .otherwise(
                    F.array(
                        F.col("properties.birthDate").cast("string")
                    )
                )
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
                birth_date_array.alias("birth_dates")
            )
            .withColumn(
                "birth_date_count",
                F.size("birth_dates")
            )
        )

        exploded_df = (
            base_df
            .withColumn(
                "birth_date_raw",
                F.explode_outer("birth_dates")
            )
            .withColumn(
                "birth_date_text",
                F.trim(
                    F.col("birth_date_raw").cast("string")
                )
            )
        )

        return (
            exploded_df
            .withColumn(
                "birth_date_format",
                self.classify_birth_date(
                    F.col("birth_date_text")
                )
            )
            .withColumn(
                "parsed_timestamp",
                self.parse_birth_date(
                    F.col("birth_date_text")
                )
            )
        )

    def get_birth_date_field(self, raw_df):
        properties_field = next(
            (
                field
                for field in raw_df.schema.fields
                if field.name == "properties"
            ),
            None
        )

        if properties_field is None:
            return None

        return next(
            (
                field
                for field in properties_field.dataType.fields
                if field.name == "birthDate"
            ),
            None
        )

    def classify_birth_date(self, value_col):
        is_empty = (
            value_col.isNull()
            | (value_col == "")
        )

        is_year_pattern = value_col.rlike(
            r"^[0-9]{4}$"
        )

        is_date_pattern = value_col.rlike(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
        )

        is_timestamp_millis_pattern = value_col.rlike(
            r"^-?[0-9]{11,14}$"
        )

        parsed_date = F.expr(
            """
            try_to_timestamp(
                birth_date_text,
                'yyyy-MM-dd'
            )
            """
        )

        timestamp_millis = F.expr(
            """
            try_cast(birth_date_text AS BIGINT)
            """
        )

        valid_year = (
            F.expr(
                "try_cast(birth_date_text AS INT)"
            )
            .between(1000, 2100)
        )

        valid_timestamp_millis = (
            timestamp_millis.isNotNull()
            & timestamp_millis.between(
                F.lit(-30610224000000),
                F.lit(4133980800000)
            )
        )

        return (
            F.when(
                is_empty,
                F.lit("empty")
            )
            .when(
                is_date_pattern & parsed_date.isNotNull(),
                F.lit("date_yyyy_mm_dd")
            )
            .when(
                is_date_pattern & parsed_date.isNull(),
                F.lit("invalid")
            )
            .when(
                is_timestamp_millis_pattern
                & valid_timestamp_millis,
                F.lit("timestamp_millis")
            )
            .when(
                is_timestamp_millis_pattern
                & ~valid_timestamp_millis,
                F.lit("invalid")
            )
            .when(
                is_year_pattern & valid_year,
                F.lit("year_only")
            )
            .when(
                is_year_pattern & ~valid_year,
                F.lit("invalid")
            )
            .otherwise(
                F.lit("invalid")
            )
        )

    def parse_birth_date(self, value_col):
        timestamp_millis = F.expr(
            "try_cast(birth_date_text AS BIGINT)"
        )

        return (
            F.when(
                F.col("birth_date_format")
                == "date_yyyy_mm_dd",
                F.expr(
                    """
                    try_to_timestamp(
                        birth_date_text,
                        'yyyy-MM-dd'
                    )
                    """
                )
            )
            .when(
                F.col("birth_date_format")
                == "timestamp_millis",
                F.timestamp_millis(timestamp_millis)
            )
            .when(
                F.col("birth_date_format")
                == "year_only",
                F.expr(
                    """
                    try_to_timestamp(
                        concat(birth_date_text, '-01-01'),
                        'yyyy-MM-dd'
                    )
                    """
                )
            )
            .otherwise(
                F.lit(None).cast("timestamp")
            )
        )

    def build_format_summary(self, birth_date_df):
        total_values = (
            birth_date_df
            .filter(
                F.col("birth_date_format") != "empty"
            )
            .count()
        )

        return (
            birth_date_df
            .groupBy("birth_date_format")
            .agg(
                F.count("*").alias("value_count"),
                F.countDistinct("entity_id").alias("entity_count"),
                F.countDistinct("birth_date_raw").alias(
                    "distinct_value_count"
                ),
                F.min("birth_date_text").alias("min_raw_value"),
                F.max("birth_date_text").alias("max_raw_value")
            )
            .withColumn(
                "percentage",
                F.when(
                    F.lit(total_values) > 0,
                    F.round(
                        F.col("value_count")
                        * F.lit(100.0)
                        / F.lit(total_values),
                        4
                    )
                )
            )
            .orderBy(
                F.desc("value_count")
            )
        )

    def build_invalid_summary(self, birth_date_df):
        return (
            birth_date_df
            .filter(
                F.col("birth_date_format") == "invalid"
            )
            .groupBy("birth_date_text")
            .agg(
                F.count("*").alias("occurrence_count"),
                F.countDistinct("entity_id").alias(
                    "entity_count"
                ),
                F.collect_set("datasets").alias(
                    "dataset_combinations"
                )
            )
            .orderBy(
                F.desc("occurrence_count"),
                "birth_date_text"
            )
        )

    def build_format_samples(self, birth_date_df):
        from pyspark.sql.window import Window

        window_spec = (
            Window
            .partitionBy("birth_date_format")
            .orderBy(
                F.col("entity_id"),
                F.col("birth_date_text")
            )
        )

        return (
            birth_date_df
            .filter(
                F.col("birth_date_format") != "empty"
            )
            .withColumn(
                "row_number",
                F.row_number().over(window_spec)
            )
            .filter(
                F.col("row_number") <= 20
            )
            .select(
                "birth_date_format",
                "entity_id",
                "entity_schema",
                "caption",
                "datasets",
                "birth_date_count",
                "birth_date_raw",
                "birth_date_text",
                "parsed_timestamp"
            )
            .orderBy(
                "birth_date_format",
                "row_number"
            )
        )

    def write_report(self, df, output_path):
        (
            df
            .write
            .mode("overwrite")
            .parquet(output_path)
        )