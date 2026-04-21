from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from adapters.iceberg_spark_adapter import iceberg_spark_adapter

spark = SparkSession.builder.getOrCreate()

bucket = "s3a://parsed-data"
output = "s3a://parsed-data-partitioned"

# 👉 List các ngày bạn muốn migrate (có thể generate động)
dates = [
    {"year": 2026, "month": 4, "day": 17},
    {"year": 2026, "month": 4, "day": 20},
    {"year": 2026, "month": 4, "day": 21},
    {"year": 2026, "month": 4, "day": 15},
]


class MigrateParsedData:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def migrate_parsed_data(self):
        for d in dates:
            input_path = f"{bucket}/{d['year']}/{str(d['month']).zfill(2)}/{str(d['day']).zfill(2)}"

            print(f"Processing {input_path}")

            df = spark.read.parquet(input_path)

            # 👉 add partition columns
            df = (
                df.withColumn("year", lit(d["year"]))
                .withColumn("month", lit(d["month"]))
                .withColumn("day", lit(d["day"]))
            )

            # 👉 write lại theo partition chuẩn
            df.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet(output)

        print("✅ Migration done")
