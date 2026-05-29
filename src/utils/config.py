"""
Centralized configuration loader for the Enterprise Data Pipeline.
Reads pipeline_config.yaml and resolves environment variable overrides.
"""

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

CONFIG_PATH = Path(__file__).parents[2] / "config" / "pipeline_config.yaml"


def _resolve_env_vars(value: Any) -> Any:
    """
    Recursively resolves ${VAR:-default} patterns in config values.
    Supports both dict and string values.
    """
    if isinstance(value, str):
        pattern = r"\$\{([^}:]+)(?::-(.*?))?\}"

        def replacer(match):
            var_name = match.group(1)
            default = match.group(2) or ""
            return os.environ.get(var_name, default)

        return re.sub(pattern, replacer, value)

    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]

    return value


@lru_cache(maxsize=1)
def get_config() -> dict:
    """
    Loads and returns the pipeline configuration.
    Results are cached — subsequent calls return the same object.

    Returns:
        Resolved configuration dictionary.
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r") as f:
        raw_config = yaml.safe_load(f)

    resolved = _resolve_env_vars(raw_config)
    return resolved


def get_storage_path(layer: str, path_key: str, **kwargs) -> str:
    """
    Returns a fully qualified S3A path for a given storage layer and path key.

    Args:
        layer: Storage layer ('bronze', 'silver', 'gold').
        path_key: Key from config storage.paths.
        **kwargs: Template variables for path formatting (e.g., date='2024-01-01').

    Returns:
        Full S3A URI, e.g., s3a://silver/orders/enriched/

    Example:
        >>> get_storage_path('bronze', 'bronze_olist', date='2024-01-15')
        's3a://bronze/olist/raw/2024-01-15/'
    """
    config = get_config()
    bucket = config["storage"]["buckets"][layer]
    path_template = config["storage"]["paths"][path_key]
    path = path_template.format(**kwargs)
    return f"s3a://{bucket}/{path}"
