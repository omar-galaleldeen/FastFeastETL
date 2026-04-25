from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_PATH = Path("config/pipeline_config.yaml")
DOTENV_PATH = Path(".env")

# Matches ${VAR_NAME} placeholders in YAML values
_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _load_dotenv() -> None:
    """
    Load key=value pairs from .env into os.environ.

    Rules:
      - Lines starting with # are comments, ignored.
      - Blank lines are ignored.
      - Values may be optionally quoted with ' or " (quotes are stripped).
      - Existing environment variables are NOT overwritten, so a value set
        in the shell before startup always wins over the .env file.
      - If .env does not exist, this is a no-op (production environments
        typically inject secrets via the shell or a secrets manager).
    """
    if not DOTENV_PATH.exists():
        return

    with DOTENV_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue

            key, _, raw_value = line.partition("=")
            key = key.strip()
            raw_value = raw_value.strip()

            # Strip surrounding quotes if present
            if len(raw_value) >= 2 and raw_value[0] in ('"', "'") and raw_value[0] == raw_value[-1]:
                raw_value = raw_value[1:-1]

            # Don't overwrite variables already set in the environment
            if key and key not in os.environ:
                os.environ[key] = raw_value


def _expand_env_vars(obj):
    """
    Recursively walk the parsed YAML structure and replace every
    ${VAR_NAME} placeholder with the value of that environment variable.
    Raises RuntimeError if a referenced variable is not set, so a
    mis-configured deployment fails loudly at startup rather than
    silently connecting with wrong credentials.
    """
    if isinstance(obj, str):
        def _replace(match):
            var = match.group(1)
            val = os.environ.get(var)
            if val is None:
                raise RuntimeError(
                    f"[config] Required environment variable '{var}' is not set. "
                    f"Add it to your .env file or export it in the shell before starting."
                )
            return val
        return _ENV_VAR_RE.sub(_replace, obj)
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(item) for item in obj]
    return obj


@lru_cache(maxsize=1)
def get_config() -> dict:
    """
    Loads pipeline_config.yaml once and caches it for the entire run.
    Every module calls get_config() — the file is only read once.

    Startup sequence:
      1. Load .env into os.environ (no-op if .env doesn't exist)
      2. Parse pipeline_config.yaml
      3. Expand ${VAR} placeholders using os.environ
      4. Cache and return the result
    """
    # Step 1: load .env so ${VAR} placeholders can be resolved
    _load_dotenv()

    # Step 2: parse YAML
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Config file not found: {CONFIG_PATH.resolve()}"
        )

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Step 3: expand placeholders
    return _expand_env_vars(raw)
