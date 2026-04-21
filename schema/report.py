from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType

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