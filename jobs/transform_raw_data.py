from pyspark.sql.functions import col, from_json, explode, concat_ws, current_timestamp, unix_timestamp, lit

from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from common.constants import RAW_BUCKET
from helpers.helpers import Helper
from schema.report import raw_content_schema, product_item_raw_schema


class TransformRawData:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def load(self):
        bucket = Helper.get_bucket(RAW_BUCKET)
        current_date = Helper.get_current_date()
        df = self.spark.read.schema(raw_content_schema).parquet(bucket)
        df = df.filter(
            (col("year") == current_date["year"]) &
            (col("month") == current_date["month"]) &
            (col("day") == current_date["day"]))
        df_parsed = (
            df
            .withColumn("json", from_json(col("content"), product_item_raw_schema))
            .withColumn("item", explode(col("json.data")))
        )
        df_final = (
            df_parsed
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
                concat_ws("_", lit("tiki"), col("productItemId"), col("shopId"))
            )
            .withColumn("crawledDate", current_timestamp())
            .withColumn("crawledDateMs", (unix_timestamp() * 1000))
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
                "thumbnailUrl"
            )
        )

        df_final.show(10)
