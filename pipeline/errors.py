"""One exception family for files that must not reach the warehouse."""

from __future__ import annotations


class FileRejected(Exception):
    """A file that cannot be loaded. `code` is a short machine label, `reason` is for humans."""

    def __init__(self, code: str, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason
