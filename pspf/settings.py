from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables.
    """
    # Valkey Configuration
    VALKEY_HOST: str = Field(default="localhost", validation_alias="VALKEY_HOST")
    VALKEY_PORT: int = Field(default=6379, validation_alias="VALKEY_PORT")
    VALKEY_DB: int = Field(default=0, validation_alias="VALKEY_DB")
    VALKEY_PASSWORD: Optional[str] = Field(default=None, validation_alias="VALKEY_PASSWORD")
    VALKEY_SSL: bool = Field(default=False, validation_alias="VALKEY_SSL")
    VALKEY_SSL_CA_CERTS: Optional[str] = Field(default=None, validation_alias="VALKEY_SSL_CA_CERTS")
    VALKEY_SSL_CERT_REQS: str = Field(default="required", validation_alias="VALKEY_SSL_CERT_REQS")

    # Telemetry
    OTEL_ENABLED: bool = Field(default=False, validation_alias="OTEL_ENABLED")
    SERVICE_NAME: str = Field(default="pspf-service", validation_alias="OTEL_SERVICE_NAME")
    PROMETHEUS_PORT: int = Field(default=8000, validation_alias="PROMETHEUS_PORT")

    # Operation Defaults
    DEFAULT_BATCH_SIZE: int = 10
    DEFAULT_POLL_INTERVAL: float = 0.1
    DLO_MAX_RETRIES: int = 3

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore"
    )

# Singleton instance
settings = Settings()
