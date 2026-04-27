import json
import sys

from pyspark.sql.functions import col, from_json, explode, concat_ws, lit, split, \
    to_timestamp, month, year, dayofmonth

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import RAW_BUCKET
from helpers.helpers import Helper
from schema.report import raw_content_schema, product_item_raw_schema


class TransformRawData:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def transform_raw_2_parsed_data(self, config):
        bucket = Helper.get_bucket(RAW_BUCKET)
        from_date = config["from_date"]
        to_date = config["to_date"]
        target_bucket = config["target_bucket"]
        from_year, from_month, from_day = [int(x) for x in from_date.split("-")]
        to_year, to_month, to_day = [int(x) for x in to_date.split("-")]
        df = self.spark.read.schema(raw_content_schema).parquet(bucket)
        df = df.filter(
            (col("year") > from_year) |
            ((col("year") == from_year) & (col("month") > from_month)) |
            ((col("year") == from_year) & (col("month") == from_month) & (col("day") >= from_day))
        ).filter(
            (col("year") < to_year) |
            ((col("year") == to_year) & (col("month") < to_month)) |
            ((col("year") == to_year) & (col("month") == to_month) & (col("day") <= to_day))
        )
        df_parsed = (
            df
            .withColumn("json", from_json(col("content"), product_item_raw_schema))
            .withColumn("item", explode(col("json.data")))
        )
        df_final = (
            df_parsed
            .withColumn(
                "normalized_source",
                split(col("source.source"), "\\.").getItem(0)
            )
            .withColumn("productItemId", col("item.id"))
            .withColumn("title", col("item.name"))
            .withColumn("link", concat_ws("", lit("https://tiki.vn/"), col("item.url_path")))
            .withColumn("shopId", col("item.seller_id"))
            .withColumn("thumbnailUrl", col("item.thumbnail_url"))
            .withColumn("sold", col("item.quantity_sold.value"))
            .withColumn("sellPrice", col("item.price").cast("float"))
            .withColumn("price", col("item.original_price").cast("float"))
            .withColumn("platformId", concat_ws("_", col("productItemId"), col("shopId")))
            .withColumn(
                "id",
                concat_ws("_", lit(col("normalized_source")), col("productItemId"), col("shopId"))
            )
            .withColumn("crawledDate", to_timestamp(col("crawledDate")))
            .withColumn("crawledDateMs", to_timestamp(col("crawledDate")))
            .withColumn("year", year(col("crawledDate")))
            .withColumn("month", month(col("crawledDate")))
            .withColumn("day", dayofmonth(col("crawledDate")))
            .select(
                "id",
                "title",
                "link",
                "platformId",
                "price",
                "sellPrice",
                "shopId",
                "sold",
                "crawledDate",
                "crawledDateMs",
                "thumbnailUrl",
                "year",
                "month",
                "day",
            )
        )
        bucket = Helper.get_bucket(target_bucket)
        df_final.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(bucket)


if __name__ == "__main__":
    job = TransformRawData()

    mode = sys.argv[1]
    config = json.loads(sys.argv[2])
    print("TransformRawData Mode: ", mode)
    print("TransformRawData Config: ", config)
    if mode == "transform_raw_2_parsed_data":
        job.transform_raw_2_parsed_data(config)
    else:
        raise ValueError("Invalid mode")
