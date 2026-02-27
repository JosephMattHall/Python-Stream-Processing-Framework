import os
import warnings
from typing import Optional, Dict, Any
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, BaseModel, model_validator

class ValkeySettings(BaseModel):
    """Configuration for Valkey/Redis backend."""
    HOST: str = "localhost"
    PORT: int = 6379
    DB: int = 0
    PASSWORD: Optional[str] = None
    SSL: bool = False
    SSL_CA_CERTS: Optional[str] = None
    SSL_CERT_REQS: str = "required"
    SOCKET_TIMEOUT: Optional[float] = 10.0

class TelemetrySettings(BaseModel):
    """Configuration for observability and management."""
    ENABLED: bool = False
    SERVICE_NAME: str = "pspf-service"
    PROMETHEUS_PORT: int = 8000
    ADMIN_PORT: int = 8001

class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables and .env files.
    """
    ENV: str = Field(default="dev", validation_alias="PSPF_ENV")
    
    # Nested configurations
    valkey: ValkeySettings = Field(default_factory=ValkeySettings)
    telemetry: TelemetrySettings = Field(default_factory=TelemetrySettings)

    # Operation Defaults
    DEFAULT_BATCH_SIZE: int = 10
    DEFAULT_POLL_INTERVAL: float = 0.1
    DLO_MAX_RETRIES: int = 3

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore",
        env_nested_delimiter="__"
    )

    @model_validator(mode='before')
    @classmethod
    def map_legacy_environment(cls, values: Any) -> Any:
        """
        Manually maps legacy environment variables into the nested structure 
        before Pydantic validation. This ensures backward compatibility.
        """
        if not isinstance(values, dict):
            return values

        # Define legacy mapping: ENV_VAR -> (nested_model_attr, field_name, type_converter)
        # Note: type_converter is optional if Pydantic can handle it downstream, 
        # but since we are inserting into a dict, we should ideally keep the types.
        legacy_map = {
            'VALKEY_HOST': ('valkey', 'HOST'),
            'VALKEY_PORT': ('valkey', 'PORT'),
            'VALKEY_DB': ('valkey', 'DB'),
            'VALKEY_PASSWORD': ('valkey', 'PASSWORD'),
            'VALKEY_SSL': ('valkey', 'SSL'),
            'VALKEY_SSL_CA_CERTS': ('valkey', 'SSL_CA_CERTS'),
            'VALKEY_SSL_CERT_REQS': ('valkey', 'SSL_CERT_REQS'),
            'VALKEY_SOCKET_TIMEOUT': ('valkey', 'SOCKET_TIMEOUT'),
            'OTEL_ENABLED': ('telemetry', 'ENABLED'),
            'OTEL_SERVICE_NAME': ('telemetry', 'SERVICE_NAME'),
            'PROMETHEUS_PORT': ('telemetry', 'PROMETHEUS_PORT'),
            'ADMIN_PORT': ('telemetry', 'ADMIN_PORT'),
        }

        # Initialize nested dicts if they don't exist
        for model_key in ('valkey', 'telemetry'):
            if model_key not in values:
                values[model_key] = {}

        # Merge from os.environ for legacy support
        for env_key, (model_key, field_key) in legacy_map.items():
            val = os.environ.get(env_key)
            if val is not None:
                # Only populate if not already provided in 'values' (e.g. via direct constructor call)
                target_dict = values[model_key]
                if field_key not in target_dict:
                    # Simple type conversion for known numeric/boolean fields
                    if field_key in ('PORT', 'DB', 'PROMETHEUS_PORT', 'ADMIN_PORT'):
                        try: target_dict[field_key] = int(val)
                        except: pass
                    elif field_key in ('SSL', 'ENABLED'):
                        target_dict[field_key] = val.lower() in ('true', '1', 'yes')
                    elif field_key == 'SOCKET_TIMEOUT':
                        try: target_dict[field_key] = float(val)
                        except: pass
                    else:
                        target_dict[field_key] = val

        return values

    @model_validator(mode='after')
    def validate_production(self) -> 'Settings':
        if self.ENV == "prod":
            if not self.valkey.PASSWORD:
                warnings.warn("VALKEY_PASSWORD is not set in production environment!")
        return self

# Singleton instance
settings = Settings()
