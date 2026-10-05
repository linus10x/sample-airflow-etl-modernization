"""Postgres access for the ingest side: the file manifest and the raw JSONB records."""

from __future__ import annotations

from datetime import date
from importlib import resources
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class Warehouse:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def _connect(self) -> psycopg.Connection[dict[str, Any]]:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def ensure_schema(self) -> None:
        ddl = resources.files("pipeline").joinpath("sql/ddl.sql").read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.execute(ddl)

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return list(conn.execute(sql, params or None))

    def status_of(self, checksum: str) -> str | None:
        rows = self.fetch_all(
            "select status from ops.file_manifest where checksum = %s", (checksum,)
        )
        return str(rows[0]["status"]) if rows else None

    def record_loaded(
        self,
        *,
        feed: str,
        path: str,
        checksum: str,
        file_date: date,
        contract_version: str,
        records: list[dict[str, Any]],
        size_bytes: int,
        run_id: str,
    ) -> bool:
        """Insert the manifest row and every record in one transaction.

        Returns False, and writes nothing, when this checksum is already known. The manifest
        insert is the gate, so two workers racing on the same file cannot both load it.
        """
        with self._connect() as conn, conn.transaction():
            inserted = conn.execute(
                """
                insert into ops.file_manifest
                    (checksum, path, feed, file_date, status, contract_version,
                     row_count, size_bytes, run_id)
                values (%s, %s, %s, %s, 'loaded', %s, %s, %s, %s)
                on conflict (checksum) do nothing
                returning checksum
                """,
                (
                    checksum,
                    path,
                    feed,
                    file_date,
                    contract_version,
                    len(records),
                    size_bytes,
                    run_id,
                ),
            ).fetchone()
            if inserted is None:
                return False
            with conn.cursor() as cur:
                cur.executemany(
                    "insert into raw.records (checksum, ordinal, feed, payload) "
                    "values (%s, %s, %s, %s)",
                    [(checksum, i, feed, Jsonb(rec)) for i, rec in enumerate(records)],
                )
        return True

    def record_quarantined(
        self,
        *,
        feed: str,
        path: str,
        checksum: str,
        file_date: date,
        reason_code: str,
        reason: str,
        size_bytes: int,
        run_id: str,
    ) -> bool:
        with self._connect() as conn, conn.transaction():
            row = conn.execute(
                """
                insert into ops.file_manifest
                    (checksum, path, feed, file_date, status, reason_code, reason,
                     size_bytes, run_id)
                values (%s, %s, %s, %s, 'quarantined', %s, %s, %s, %s)
                on conflict (checksum) do nothing
                returning checksum
                """,
                (checksum, path, feed, file_date, reason_code, reason, size_bytes, run_id),
            ).fetchone()
        return row is not None

    def latest_loaded_date(self, feed: str, up_to: date) -> date | None:
        rows = self.fetch_all(
            """
            select max(file_date) as latest from ops.file_manifest
            where feed = %s and status = 'loaded' and file_date <= %s
            """,
            (feed, up_to),
        )
        latest = rows[0]["latest"]
        return latest if isinstance(latest, date) else None

    def reset(self) -> None:
        """Drop everything the pipeline and dbt created. For demos and tests only."""
        with self._connect() as conn:
            for schema in ("raw", "ops", "staging", "intermediate", "marts", "seeds"):
                conn.execute(f"drop schema if exists {schema} cascade")
