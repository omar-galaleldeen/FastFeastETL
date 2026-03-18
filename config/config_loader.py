from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_PATH = Path("config/pipeline_config.yaml")


@lru_cache(maxsize=1)
def get_config() -> dict:
    """
    Loads pipeline_config.yaml once and caches it for the entire run.
    Every module calls get_config() — the file is only read once.
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Config file not found: {CONFIG_PATH.resolve()}"
        )

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
