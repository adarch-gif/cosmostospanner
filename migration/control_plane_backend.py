from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse


SPANNER_CONTROL_PLANE_PREFIX = "spanner://"
SPANNER_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


@dataclass(frozen=True)
class SpannerControlPlaneRef:
    project: str
    instance: str
    database: str
    table: str
    namespace: str

    def same_table(self, other: "SpannerControlPlaneRef") -> bool:
        return (
            self.project == other.project
            and self.instance == other.instance
            and self.database == other.database
            and self.table == other.table
        )


@dataclass(frozen=True)
class ControlPlaneRow:
    record_key: str
    payload: dict[str, Any]
    status: str = ""
    owner_id: str = ""
    lease_expires_at: str = ""
    updated_at: str = ""


def is_spanner_control_plane_path(location: str) -> bool:
    return str(location).startswith(SPANNER_CONTROL_PLANE_PREFIX)


def parse_spanner_control_plane_path(location: str) -> SpannerControlPlaneRef:
    parsed = urlparse(location)
    if parsed.scheme != "spanner":
        raise ValueError(f"Not a Spanner control-plane path: {location}")
    path_parts = [part for part in parsed.path.split("/") if part]
    if not parsed.netloc or len(path_parts) != 3:
        raise ValueError(
            "Spanner control-plane paths must use "
            "spanner://<project>/<instance>/<database>/<table>?namespace=<name>."
        )
    query = parse_qs(parsed.query)
    namespace = str(query.get("namespace", [""])[0]).strip()
    if not namespace:
        raise ValueError(
            "Spanner control-plane paths must include ?namespace=<name>."
        )
    table = path_parts[2]
    if not SPANNER_IDENTIFIER_RE.fullmatch(table):
        raise ValueError(
            "Spanner control-plane table must be a valid Spanner identifier. "
            f"Received: {table!r}"
        )
    return SpannerControlPlaneRef(
        project=parsed.netloc,
        instance=path_parts[0],
        database=path_parts[1],
        table=table,
        namespace=namespace,
    )


class SpannerControlPlaneBackend:
    _COLUMNS = [
        "namespace",
        "record_key",
        "payload_json",
        "status",
        "owner_id",
        "lease_expires_at",
        "updated_at",
    ]

    def __init__(self, location: str) -> None:
        self.ref = parse_spanner_control_plane_path(location)
        self._database = self._create_database()

    def _create_database(self):
        from google.cloud import spanner

        client = spanner.Client(project=self.ref.project)
        return client.instance(self.ref.instance).database(self.ref.database)

    def _commit_timestamp_value(self):
        from google.cloud import spanner

        return spanner.COMMIT_TIMESTAMP

    def _row_to_record(self, row: Any) -> ControlPlaneRow:
        payload_raw = row[2]
        payload = json.loads(str(payload_raw)) if payload_raw else {}
        lease_expires_at = row[5]
        updated_at = row[6]
        return ControlPlaneRow(
            record_key=str(row[1]),
            payload=payload,
            status=str(row[3] or ""),
            owner_id=str(row[4] or ""),
            lease_expires_at=lease_expires_at.isoformat()
            if isinstance(lease_expires_at, datetime)
            else str(lease_expires_at or ""),
            updated_at=updated_at.isoformat()
            if isinstance(updated_at, datetime)
            else str(updated_at or ""),
        )

    def _read_records_with_reader(
        self,
        reader: Any,
        record_keys: list[str],
    ) -> dict[str, ControlPlaneRow]:
        if not record_keys:
            return {}
        from google.cloud.spanner_v1 import KeySet

        keyset = KeySet(keys=[(self.ref.namespace, key) for key in record_keys])
        rows = reader.read(
            table=self.ref.table,
            columns=self._COLUMNS,
            keyset=keyset,
        )
        records: dict[str, ControlPlaneRow] = {}
        for row in rows:
            record = self._row_to_record(row)
            records[record.record_key] = record
        return records

    def get(self, record_key: str) -> ControlPlaneRow | None:
        with self._database.snapshot() as snapshot:
            return self.get_with_reader(snapshot, record_key)

    def get_many(self, record_keys: list[str]) -> dict[str, ControlPlaneRow]:
        with self._database.snapshot() as snapshot:
            return self._read_records_with_reader(snapshot, record_keys)

    def list_namespace(self) -> dict[str, ControlPlaneRow]:
        from google.cloud.spanner_v1 import param_types

        sql = (
            f"SELECT namespace, record_key, payload_json, status, owner_id, "  # nosec B608
            f"lease_expires_at, updated_at FROM `{self.ref.table}` "
            "WHERE namespace = @namespace ORDER BY record_key"
        )
        with self._database.snapshot() as snapshot:
            rows = snapshot.execute_sql(
                sql=sql,
                params={"namespace": self.ref.namespace},
                param_types={"namespace": param_types.STRING},
            )
            records: dict[str, ControlPlaneRow] = {}
            for row in rows:
                record = self._row_to_record(row)
                records[record.record_key] = record
            return records

    def list_prefix(self, record_key_prefix: str) -> dict[str, ControlPlaneRow]:
        from google.cloud.spanner_v1 import param_types

        sql = (
            f"SELECT namespace, record_key, payload_json, status, owner_id, "  # nosec B608
            f"lease_expires_at, updated_at FROM `{self.ref.table}` "
            "WHERE namespace = @namespace AND STARTS_WITH(record_key, @record_key_prefix) "
            "ORDER BY record_key"
        )
        with self._database.snapshot() as snapshot:
            rows = snapshot.execute_sql(
                sql=sql,
                params={
                    "namespace": self.ref.namespace,
                    "record_key_prefix": record_key_prefix,
                },
                param_types={
                    "namespace": param_types.STRING,
                    "record_key_prefix": param_types.STRING,
                },
            )
            records: dict[str, ControlPlaneRow] = {}
            for row in rows:
                record = self._row_to_record(row)
                records[record.record_key] = record
            return records

    def get_with_reader(self, reader: Any, record_key: str) -> ControlPlaneRow | None:
        return self._read_records_with_reader(reader, [record_key]).get(record_key)

    def upsert(
        self,
        transaction: Any,
        *,
        record_key: str,
        payload: dict[str, Any],
        status: str = "",
        owner_id: str = "",
        lease_expires_at: datetime | None = None,
    ) -> None:
        transaction.insert_or_update(
            table=self.ref.table,
            columns=self._COLUMNS,
            values=[
                (
                    self.ref.namespace,
                    record_key,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
                    status,
                    owner_id,
                    lease_expires_at,
                    self._commit_timestamp_value(),
                )
            ],
        )

    def delete(self, transaction: Any, *, record_key: str) -> None:
        from google.cloud.spanner_v1 import KeySet

        transaction.delete(
            self.ref.table,
            KeySet(keys=[(self.ref.namespace, record_key)]),
        )

    def run_in_transaction(self, fn: Callable[[Any], Any]) -> Any:
        return self._database.run_in_transaction(fn)
