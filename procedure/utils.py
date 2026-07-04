from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id() -> str:
    return str(uuid4())


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any, length: int | None = None) -> str:
    digest = hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()
    if length is None:
        return digest
    return digest[:length]


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def lower_snake(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", value)
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        cleaned = "column"
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned

