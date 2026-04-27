from pathlib import Path
from dotenv import load_dotenv
from pydantic import field_validator, Field
from pydantic_settings import SettingsConfigDict, BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

class Settings(BaseSettings):
    app_env: str = Field(default="development", description="ENV", alias="APP_ENV")
    #Minio setting
    minio_endpoint: str = Field(default="http://localhost:9018", description="Minio endpoint", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="", description="Minio access key", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="", description="Minio secret key", alias="MINIO_SECRET_KEY")
    kafka_host: str = Field(default="localhost:9092", description="Kafka host", alias="KAFKA_HOST")
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("app_env")
    @classmethod
    def validate_environment(cls, env: str):
        allowed_values = ["local", "dev", "test", "staging", "prod"]
        if env not in allowed_values:
            raise ValueError("Environment is nott allowed")
        return env


settings = Settings()