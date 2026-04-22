from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, ArrayType, \
    DoubleType

last_state_product_item_schema = StructType([
    StructField("id", StringType(), False),
    StructField("crawledDateMs", TimestampType(), False),
    StructField("sold", IntegerType(), False),
])

product_item_daily_schema = StructType([
    StructField("id", StringType(), False),
    StructField("sold", IntegerType(), False),
    StructField("price", FloatType(), False),
    StructField("sellPrice", FloatType(), True),
    StructField("crawledDateMs", TimestampType(), False),
])

parsed_product_item_schema = StructType([
    StructField("id", StringType(), False),
    StructField("sold", IntegerType(), False),
    StructField("price", FloatType(), True),
    StructField("sellPrice", FloatType(), True),
    StructField("crawledDateMs", TimestampType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
])

source_raw_schema = StructType([
    StructField("id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("link", StringType(), False),
    StructField("name", StringType(), False),
    StructField("platformShopId", StringType(), False),
    StructField("limit", IntegerType(), False),
    StructField("page", IntegerType(), False),
])

raw_content_schema = StructType([
    StructField("content", StringType(), False),
    StructField("crawledDate", StringType(), False),
    StructField("source", source_raw_schema, False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False),
])

product_item_raw_schema = StructType([
    StructField("data", ArrayType(
        StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("url_path", StringType()),
            StructField("seller_id", StringType()),
            StructField("thumbnail_url", StringType()),
            StructField("price", DoubleType()),
            StructField("original_price", DoubleType()),
            StructField("quantity_sold", StructType([
                StructField("value", IntegerType())
            ]))
        ])
    ))

])
