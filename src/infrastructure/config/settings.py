"""
Centralized application settings using pydantic-settings.

All configuration is read from environment variables / a `.env` file.
Nothing else in the codebase should call `os.environ` directly - this is
the single source of truth for configuration.
"""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    topic_name: str = Field(..., alias="KAFKA_TOPIC_NAME")
    client_id: str = Field(..., alias="KAFKA_CLIENT_ID")
    group_id: str = Field(default="python-api-consumer-group", alias="KAFKA_GROUP_ID")


class RabbitMQSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RABBITMQ_", extra="ignore")

    host: str = Field(..., alias="RABBITMQ_HOST")
    port: int = Field(..., alias="RABBITMQ_PORT")
    user: str = Field(..., alias="RABBITMQ_USER")
    password: str = Field(..., alias="RABBITMQ_PASSWORD")
    users_queue_name: str = Field(..., alias="USERS_QUEUE_NAME")

    @property
    def url(self) -> str:
        return f"amqp://{self.user}:{self.password}@{self.host}:{self.port}/"


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore")

    user: str = Field(..., alias="POSTGRES_USER")
    password: str = Field(..., alias="POSTGRES_PASSWORD")
    host: str = Field(..., alias="POSTGRES_HOST")
    port: int = Field(..., alias="POSTGRES_PORT")
    db: str = Field(..., alias="POSTGRES_DB")

    @property
    def async_dsn(self) -> str:
        """DSN using the asyncpg driver, for use with SQLAlchemy's async engine."""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )

    @property
    def sync_dsn(self) -> str:
        """Plain psycopg2-style DSN, used by Alembic (which runs sync migrations)."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class Settings(BaseSettings):
    """Root settings object. Reads from `.env` at the project root."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    app_name: str = Field(default="events-worker", alias="APP_NAME")

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    rabbitmq: RabbitMQSettings = Field(default_factory=RabbitMQSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)


@lru_cache
def get_settings() -> Settings:
    """Cached settings accessor - settings are read from disk/env only once."""
    return Settings()
