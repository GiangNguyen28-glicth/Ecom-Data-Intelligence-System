from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from helpers.helpers import Helper
from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    broadcast,
    countDistinct
)


class Opensanctions:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def process(self,config):
        parsed_bucket = config["bucket"]
        bucket = Helper.get_bucket(parsed_bucket)
        x = self.spark.read.json(f"{bucket}/entities.ftm.json")
        y_raw = self.spark.read.json(f"{bucket}/hr_peps.jsonl")
        entity_schema = x.schema
        y = (
            y_raw
            .withColumn(
                "entity",
                from_json(col("information"), entity_schema)
            )
            .select("entity.*")
        )
        # ==========================
        # Filter Y
        # ==========================
        y_filtered = (
            y.filter(
                col("id").startswith("NK") |
                col("id").startswith("Q")
            )
        )

        # ------------------------------------------------------------------
        # Missing IDs
        # ------------------------------------------------------------------
        missing = (
            broadcast(y_filtered)
            .join(
                x.select("id"),
                "id",
                "left_anti"
            )
        )

        # ------------------------------------------------------------------
        # Explode referents
        # ------------------------------------------------------------------
        referent_index = (
            x
            .select(
                col("id").alias("parent_entity_id"),
                explode("referents").alias("referent")
            )
        )

        # ------------------------------------------------------------------
        # Find missing ids inside referents
        # ------------------------------------------------------------------
        referent_matches = (
            broadcast(missing.alias("m"))
            .join(
                referent_index.alias("r"),
                col("m.id") == col("r.referent"),
                "inner"
            )
            .join(
                x.select(
                    col("id").alias("entity_id"),
                    col("schema").alias("parent_schema"),
                    col("caption").alias("parent_caption")
                ),
                col("parent_entity_id") == col("entity_id"),
                "left"
            )
            .drop("entity_id")
        )

        # ------------------------------------------------------------------
        # Statistics
        # ------------------------------------------------------------------
        total_filtered = y_filtered.count()
        total_missing = missing.count()
        total_found_in_referents = referent_matches.select("id").distinct().count()

        print("=" * 80)
        print(f"Filtered Y (NK/Q):                {total_filtered}")
        print(f"Missing from X.id:                {total_missing}")
        print(f"Found in X.referents:             {total_found_in_referents}")
        print(f"Still completely missing:         {total_missing - total_found_in_referents}")
        print("=" * 80)

        print("\n===== Missing sample =====")
        missing.show(20, truncate=False)

        print("\n===== Referent match sample =====")
        referent_matches.show(20, truncate=False)

        self.spark.stop()