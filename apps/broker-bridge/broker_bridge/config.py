"""Configuration for broker-bridge service loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Broker-bridge configuration.

    All fields are loaded from environment variables.  Defaults are provided
    for the IB Gateway connection; POSTGRES_URL and REDIS_URL must be supplied
    explicitly.
    """

    IB_HOST: str = "127.0.0.1"
    IB_PORT: int = 4002
    IB_CLIENT_ID: int = 1
    POSTGRES_URL: str
    DB_SSL: str = ""
    REDIS_URL: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()  # type: ignore[call-arg]
