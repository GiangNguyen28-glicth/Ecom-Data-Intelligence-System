import datetime

class Helper:
    @staticmethod
    def get_bucket(bucket_name) -> str:
        return f's3a://{bucket_name}'

    @staticmethod
    def get_current_date() -> dict:
        now = datetime.datetime.now()
        return {
            'year': now.year,
            'month': str(now.month).zfill(2),
            'day': str(now.day).zfill(2),
            'timestamp': now.isoformat()
        }