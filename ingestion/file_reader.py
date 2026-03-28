from __future__ import annotations

import pandas as pd
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)


from config.config_loader import get_config

_cfg          = get_config()
FILE_TYPE_MAP : dict[str, str] = _cfg["reader"]["file_type_map"]



# ─── Private readers ──────────────────────────────────────────────────────────

def _read_csv(path: Path) -> pd.DataFrame:
    """
    Reads a CSV file into a DataFrame.
    dtype=str — every column stays as raw string.
    keep_default_na=False — empty strings stay as "" not NaN.
    Validation layer decides what is null and what is not.
    """
    return pd.read_csv(
        path,
        dtype           = str,
        keep_default_na = False,
        encoding        = "utf-8",
    )


def _read_json(path: Path) -> pd.DataFrame:
    """
    Reads a JSON file into a DataFrame.
    dtype=str — every column stays as raw string.
    Handles both:
      - array of objects  [ {...}, {...} ]   → orient='records'
      - single object     { ... }            → wrapped in list first
    """
    import json

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # normalize single object → list so read_json always gets a list
    if isinstance(raw, dict):
        raw = [raw]

    return pd.DataFrame(raw).astype(str)


# ─── Public API ───────────────────────────────────────────────────────────────

def get_file_type(filename: str) -> str:
    """
    Maps a filename to its logical type.
    Raises ValueError if the file is not in FILE_TYPE_MAP.
    Called by read_file() — runner never needs to call this directly.
    """
    file_type = FILE_TYPE_MAP.get(filename)

    if file_type is None:
        raise ValueError(
            f"[reader] unknown file: '{filename}' — not in FILE_TYPE_MAP"
        )

    return file_type


def read_file(path: Path) -> tuple[pd.DataFrame, str]:
    """
    Reads a file and returns (DataFrame, file_type).

    file_type tells the runner what data is inside:
        "customers", "orders", "tickets", etc.

    Raises:
        ValueError  — unknown file name or unsupported format
        Exception   — any pandas / IO read error
        (caller catches all and handles appropriately)
    """
    filename  = path.name
    suffix    = path.suffix.lower()
    file_type = get_file_type(filename)      # raises ValueError if unknown

    if suffix == ".csv":
        df = _read_csv(path)

    elif suffix == ".json":
        df = _read_json(path)

    else:
        raise ValueError(
            f"[reader] unsupported format '{suffix}' for file: {filename}"
        )

    return df, file_type