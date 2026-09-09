from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["local", "test", "production"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_", env_file=".env", extra="ignore")

    env: Environment = "local"
    log_level: LogLevel = "INFO"


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 5432
    user: str = "orders"
    password: SecretStr = SecretStr("orders")
    name: str = "orders"
    pool_size: int = 10
    max_overflow: int = 5
    echo: bool = False

    @property
    def url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=".env", extra="ignore")

    bootstrap_servers: str = "localhost:9092"
    orders_topic: str = "orders.events"
    dlq_topic: str = "orders.events.dlq"
    consumer_group_id: str = "notifications"
    producer_linger_ms: int = 5
    producer_request_timeout_ms: int = 5000


class OutboxSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OUTBOX_", env_file=".env", extra="ignore")

    batch_size: int = 50
    poll_interval_seconds: float = 0.2
    max_attempts: int = 5
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 30.0
    publish_timeout_seconds: float = 10.0


class ConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CONSUMER_", env_file=".env", extra="ignore")

    max_attempts: int = 5
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 30.0


class Settings(BaseSettings):
    app: AppSettings = Field(default_factory=AppSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    outbox: OutboxSettings = Field(default_factory=OutboxSettings)
    consumer: ConsumerSettings = Field(default_factory=ConsumerSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
