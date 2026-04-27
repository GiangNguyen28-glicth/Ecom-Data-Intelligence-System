from datetime import datetime, timedelta
from functools import reduce
from pyspark.sql.functions import col


class Helper:
    @staticmethod
    def get_bucket(bucket_name) -> str:
        return f's3a://{bucket_name}'

    @staticmethod
    def get_current_date() -> dict:
        now = datetime.now()
        return {
            'year': now.year,
            'month': str(now.month).zfill(2),
            'day': str(now.day).zfill(2),
            'timestamp': now.isoformat()
        }

    @staticmethod
    def get_paths_by_minio_format_n_time_range(from_date: str, to_date: str, bucket: str) -> list[str]:
        start = datetime.strptime(from_date, "%Y-%m-%d")
        end = datetime.strptime(to_date, "%Y-%m-%d")

        paths = []
        current = start

        while current <= end:
            paths.append(
                f"{bucket}/year={current.year}/month=0{current.month}/day={current.day}"
            )
            current += timedelta(days=1)
        return paths

    @staticmethod
    def build_partition_filter(from_date, to_date):
        start = datetime.strptime(from_date, "%Y-%m-%d")
        end = datetime.strptime(to_date, "%Y-%m-%d")

        conditions = []

        current = start
        while current <= end:
            conditions.append(
                (col("year") == current.year) &
                (col("month") == current.month) &
                (col("day") == current.day)
            )
            current += timedelta(days=1)

        return reduce(lambda a, b: a | b, conditions)
