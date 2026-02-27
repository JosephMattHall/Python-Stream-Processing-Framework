import os
import json
from typing import Optional, Dict, Any
from pathlib import Path
import yaml # type: ignore
from pspf.settings import Settings, settings as default_settings
from pspf.utils.logging import get_logger

logger = get_logger("ConfigLoader")

def load_config_from_file(file_path: str) -> Settings:
    """
    Loads configuration from a YAML or JSON file and merges it with defaults/environment.
    
    Args:
        file_path: Path to the configuration file.
        
    Returns:
        Settings: A populated Settings instance.
    """
    path = Path(file_path)
    if not path.exists():
        logger.warning(f"Config file {file_path} not found. Using defaults.")
        return default_settings

    try:
        with open(path, "r") as f:
            if path.suffix in (".yaml", ".yml"):
                data = yaml.safe_load(f)
            elif path.suffix == ".json":
                data = json.load(f)
            else:
                raise ValueError(f"Unsupported config file format: {path.suffix}")

        if not data:
            return default_settings

        # Merge with environment variables via Pydantic
        # Pydantic Settings allows passing a dict to the constructor to override defaults
        return Settings(**data)
    except Exception as e:
        logger.error(f"Failed to load config from {file_path}: {e}")
        return default_settings

def get_config() -> Settings:
    """
    Retrieves the global configuration. 
    Checks PSPF_CONFIG_PATH environment variable for a file-based override.
    """
    config_path = os.environ.get("PSPF_CONFIG_PATH")
    if config_path:
        return load_config_from_file(config_path)
    return default_settings
