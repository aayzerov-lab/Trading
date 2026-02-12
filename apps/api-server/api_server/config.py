"""Configuration for api-server service loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """API server configuration.

    All fields are loaded from environment variables.  POSTGRES_URL and
    REDIS_URL must be supplied explicitly; the remaining fields have sensible
    defaults for local development.
    """

    POSTGRES_URL: str
    REDIS_URL: str
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    FRED_API_KEY: str = ""  # Optional, needed for FRED data fetching
    OPENAI_API_KEY: str = ""  # Optional, gated LLM summariser for high-priority events
    EDGAR_USER_AGENT: str = "TradingWorkstation admin@localhost"  # SEC EDGAR User-Agent

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()  # type: ignore[call-arg]
