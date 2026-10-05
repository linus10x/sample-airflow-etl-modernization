"""Bytes in, checked JSON out. No warehouse or S3 knowledge lives here."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pipeline.errors import FileRejected


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_json_bytes(data: bytes) -> Any:
    if not data.strip():
        raise FileRejected("empty_file", "file is empty")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FileRejected(
            "invalid_encoding", f"file is not valid UTF-8 (byte {exc.start})"
        ) from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise FileRejected(
            "invalid_json", f"not valid JSON: {exc.msg} at line {exc.lineno} column {exc.colno}"
        ) from exc
