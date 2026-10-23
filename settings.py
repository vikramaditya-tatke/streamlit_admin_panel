from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    clickhouse_base_url: str
    clickhouse_batch_size: int
    clickhouse_compression_protocol: str
    clickhouse_password: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_database: str = "default"
    clickhouse_secure: bool = True
    model_config = SettingsConfigDict(env_file=".env")
