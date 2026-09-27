"""Atomic file writers.

Every writer writes to a sibling ``<name>.part`` file first, then calls
``os.replace()`` to move it into place. Readers therefore never observe a
partially-written file, and re-running a pipeline step cannot leave a
corrupt output behind.

The ``.part`` suffix is gitignored (see .gitignore).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

_PART_SUFFIX = ".part"


def _tmp_path(dest: Path) -> Path:
    """Return the sibling temp path used during an atomic write.

    Uses string concatenation (not ``with_suffix``) so that filenames with
    multiple dots — e.g. ``ra_master.parquet`` — keep their full suffix.
    """
    return dest.with_name(dest.name + _PART_SUFFIX)


def _finalize(tmp: Path, dest: Path) -> Path:
    """Atomically move ``tmp`` into place and return ``dest``."""
    os.replace(tmp, dest)  # atomic on POSIX and Windows (same filesystem)
    return dest


def atomic_write_bytes(data: bytes, dest: Path) -> Path:
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(dest)
    try:
        tmp.write_bytes(data)
        _finalize(tmp, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    logger.info("wrote %s (%d bytes)", dest, dest.stat().st_size)
    return dest


def atomic_write_text(text: str, dest: Path, encoding: str = "utf-8") -> Path:
    return atomic_write_bytes(text.encode(encoding), dest)


def atomic_write_json(obj: Any, dest: Path, indent: int = 2) -> Path:
    return atomic_write_text(json.dumps(obj, indent=indent, default=str), dest)


def atomic_write_parquet(df: pd.DataFrame, dest: Path, **kwargs: Any) -> Path:
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(dest)
    try:
        df.to_parquet(tmp, index=False, **kwargs)
        _finalize(tmp, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    logger.info("wrote %s (%d rows)", dest, len(df))
    return dest


def atomic_write_csv(df: pd.DataFrame, dest: Path, **kwargs: Any) -> Path:
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(dest)
    try:
        df.to_csv(tmp, index=False, **kwargs)
        _finalize(tmp, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    logger.info("wrote %s (%d rows)", dest, len(df))
    return dest