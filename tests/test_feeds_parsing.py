from datetime import date

import pytest

from pipeline.errors import FileRejected
from pipeline.feeds import (
    incoming_key,
    parse_incoming_key,
    quarantine_key,
    reason_key,
)
from pipeline.parsing import parse_json_bytes, sha256_hex


def test_key_round_trip() -> None:
    key = incoming_key("vendor_a", date(2026, 3, 5))
    assert key == "incoming/vendor_a/2026-03-05.json"
    assert parse_incoming_key(key) == ("vendor_a", date(2026, 3, 5))


@pytest.mark.parametrize(
    "key",
    [
        "incoming/vendor_a/latest.json",
        "incoming/vendor_a/2026-13-40.json",
        "other/x.json",
        "incoming/vendor_a/2026-03-05.json.tmp",
    ],
)
def test_malformed_keys_do_not_parse(key: str) -> None:
    assert parse_incoming_key(key) is None


def test_quarantine_keys_mirror_incoming() -> None:
    assert (
        quarantine_key("incoming/vendor_c/2026-03-05.json") == "quarantine/vendor_c/2026-03-05.json"
    )
    assert quarantine_key("stray.json") == "quarantine/stray.json"
    assert reason_key("incoming/vendor_c/2026-03-05.json") == (
        "quarantine/vendor_c/2026-03-05.json.reason.json"
    )


def test_checksum_is_stable_and_content_sensitive() -> None:
    assert sha256_hex(b"abc") == sha256_hex(b"abc")
    assert sha256_hex(b"abc") != sha256_hex(b"abd")
    assert len(sha256_hex(b"")) == 64


def test_parse_valid_json() -> None:
    assert parse_json_bytes(b'{"a": 1}') == {"a": 1}


@pytest.mark.parametrize(
    ("data", "code"),
    [
        (b"", "empty_file"),
        (b"   \n", "empty_file"),
        (b"\xff\xfe", "invalid_encoding"),
        (b'{"a": ', "invalid_json"),
    ],
)
def test_parse_rejects_unreadable_files(data: bytes, code: str) -> None:
    with pytest.raises(FileRejected) as info:
        parse_json_bytes(data)
    assert info.value.code == code
    assert info.value.reason
