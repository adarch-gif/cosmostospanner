from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
import re
from types import ModuleType

import pytest

from migration.resume import ReaderCursorStore, StreamResumeState
from migration.state_store import WatermarkStore


def _install_fake_spanner(monkeypatch: pytest.MonkeyPatch) -> None:
    commit_timestamp = object()
    store: dict[tuple[str, str, str], tuple] = {}
    tick = {"count": 0}

    class FakeKeySet:
        def __init__(
            self,
            *,
            keys: list[tuple[str, str]] | None = None,
            all_: bool = False,
        ) -> None:
            self.keys = keys or []
            self.all_ = all_

    class _BaseReader:
        def read(self, *, table: str, columns: list[str], keyset: FakeKeySet):
            del columns
            rows: list[tuple] = []
            if keyset.all_:
                for (row_table, _namespace, _record_key), row in store.items():
                    if row_table == table:
                        rows.append(row)
                return rows
            for namespace, record_key in keyset.keys:
                row = store.get((table, namespace, record_key))
                if row is not None:
                    rows.append(row)
            return rows

        def execute_sql(self, *, sql: str, params: dict[str, object], param_types: dict[str, object]):
            del param_types
            match = re.search(r"FROM `([^`]+)`", sql)
            assert match is not None
            table = match.group(1)
            namespace = str(params.get("namespace", ""))
            rows: list[tuple] = []
            for (row_table, row_namespace, _record_key), row in sorted(store.items()):
                if row_table == table and row_namespace == namespace:
                    rows.append(row)
            return rows

    class FakeSnapshot(_BaseReader):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    class FakeTransaction(_BaseReader):
        def insert_or_update(self, *, table: str, columns: list[str], values: list[tuple]) -> None:
            del columns
            for value in values:
                namespace = str(value[0])
                record_key = str(value[1])
                payload_json = str(value[2])
                status = str(value[3] or "")
                owner_id = str(value[4] or "")
                lease_expires_at = value[5]
                updated_at = value[6]
                if updated_at is commit_timestamp:
                    tick["count"] += 1
                    updated_at = datetime(2026, 3, 13, tzinfo=UTC) + timedelta(
                        seconds=tick["count"]
                    )
                store[(table, namespace, record_key)] = (
                    namespace,
                    record_key,
                    payload_json,
                    status,
                    owner_id,
                    lease_expires_at,
                    updated_at,
                )

        def delete(self, table: str, keyset: FakeKeySet) -> None:
            for namespace, record_key in keyset.keys:
                store.pop((table, namespace, record_key), None)

    class FakeDatabase:
        def snapshot(self) -> FakeSnapshot:
            return FakeSnapshot()

        def run_in_transaction(self, fn):
            return fn(FakeTransaction())

    class FakeInstance:
        def database(self, database_name: str) -> FakeDatabase:
            del database_name
            return FakeDatabase()

    class FakeSpannerClient:
        def __init__(self, project: str) -> None:
            self.project = project

        def instance(self, instance_name: str) -> FakeInstance:
            del instance_name
            return FakeInstance()

    google_module = sys.modules.get("google", ModuleType("google"))
    cloud_module = ModuleType("google.cloud")
    spanner_module = ModuleType("google.cloud.spanner")
    spanner_v1_module = ModuleType("google.cloud.spanner_v1")
    param_types_module = ModuleType("google.cloud.spanner_v1.param_types")

    spanner_module.Client = FakeSpannerClient
    spanner_module.COMMIT_TIMESTAMP = commit_timestamp
    spanner_v1_module.KeySet = FakeKeySet
    param_types_module.STRING = "STRING"
    spanner_v1_module.param_types = param_types_module
    cloud_module.spanner = spanner_module
    cloud_module.spanner_v1 = spanner_v1_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.spanner", spanner_module)
    monkeypatch.setitem(sys.modules, "google.cloud.spanner_v1", spanner_v1_module)
    monkeypatch.setitem(sys.modules, "google.cloud.spanner_v1.param_types", param_types_module)


def _install_fake_gcs(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[tuple[str, str], dict[str, object]] = {}

    class FakeNotFound(Exception):
        pass

    class FakePreconditionFailed(Exception):
        pass

    class FakeBlob:
        def __init__(self, bucket_name: str, blob_name: str) -> None:
            self._key = (bucket_name, blob_name)
            self.generation: int | None = None

        def reload(self) -> None:
            if self._key not in store:
                raise FakeNotFound()
            self.generation = int(store[self._key]["generation"])

        def download_as_text(self, encoding: str = "utf-8") -> str:
            del encoding
            current = store[self._key]
            self.generation = int(current["generation"])
            return str(current["payload"])

        def upload_from_string(
            self,
            payload: str,
            *,
            content_type: str,
            if_generation_match: int,
        ) -> None:
            del content_type
            current = store.get(self._key)
            current_generation = int(current["generation"]) if current else None
            expected_generation = 0 if current_generation is None else current_generation
            if if_generation_match != expected_generation:
                raise FakePreconditionFailed()
            next_generation = 1 if current_generation is None else current_generation + 1
            store[self._key] = {"payload": payload, "generation": next_generation}
            self.generation = next_generation

    class FakeBucket:
        def __init__(self, name: str) -> None:
            self._name = name

        def blob(self, blob_name: str) -> FakeBlob:
            return FakeBlob(self._name, blob_name)

    class FakeStorageClient:
        def bucket(self, bucket_name: str) -> FakeBucket:
            return FakeBucket(bucket_name)

    google_module = sys.modules.get("google", ModuleType("google"))
    cloud_module = ModuleType("google.cloud")
    storage_module = ModuleType("google.cloud.storage")
    api_core_module = ModuleType("google.api_core")
    exceptions_module = ModuleType("google.api_core.exceptions")

    storage_module.Client = FakeStorageClient
    exceptions_module.NotFound = FakeNotFound
    exceptions_module.PreconditionFailed = FakePreconditionFailed
    cloud_module.storage = storage_module
    api_core_module.exceptions = exceptions_module
    google_module.cloud = cloud_module
    google_module.api_core = api_core_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)
    monkeypatch.setitem(sys.modules, "google.api_core", api_core_module)
    monkeypatch.setitem(sys.modules, "google.api_core.exceptions", exceptions_module)


def test_watermark_store_roundtrip(tmp_path) -> None:
    path = tmp_path / "watermarks.json"
    store = WatermarkStore(str(path))
    store.set("users", 123)
    store.flush()

    reloaded = WatermarkStore(str(path))
    assert reloaded.get("users") == 123


def test_watermark_store_rejects_corrupted_json(tmp_path) -> None:
    path = tmp_path / "watermarks.json"
    path.write_text("{bad json", encoding="utf-8")
    with pytest.raises(ValueError, match="corrupted"):
        WatermarkStore(str(path))


def test_watermark_store_roundtrip_gcs(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_gcs(monkeypatch)

    first = WatermarkStore("gs://migration-state/watermarks.json")
    second = WatermarkStore("gs://migration-state/watermarks.json")

    first.set("users", 123)
    second.set("orders", 456)

    first.flush()
    second.flush()

    reloaded = WatermarkStore("gs://migration-state/watermarks.json")
    assert reloaded.get("users") == 123
    assert reloaded.get("orders") == 456


def test_reader_cursor_store_roundtrip(tmp_path) -> None:
    path = tmp_path / "reader-cursors.json"
    store = ReaderCursorStore(str(path))
    store.set(
        "v1:users->Users",
        StreamResumeState(
            last_source_key="u2",
            last_watermark=12,
            emitted_count=2,
            page_start_token="page-1",
            scope="scope-a",
            updated_at="2026-03-13T00:00:00+00:00",
        ),
    )
    store.flush()

    reloaded = ReaderCursorStore(str(path)).get("v1:users->Users")
    assert reloaded.last_source_key == "u2"
    assert reloaded.last_watermark == 12
    assert reloaded.page_start_token == "page-1"
    assert reloaded.scope == "scope-a"


def test_reader_cursor_store_roundtrip_gcs(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_gcs(monkeypatch)

    first = ReaderCursorStore("gs://migration-state/reader-cursors.json")
    second = ReaderCursorStore("gs://migration-state/reader-cursors.json")

    first.set(
        "v1:users->Users",
        StreamResumeState(
            last_source_key="u1",
            last_watermark=10,
            emitted_count=1,
            scope="scope-a",
            updated_at="2026-03-13T00:00:00+00:00",
        ),
    )
    second.set(
        "v1:orders->Orders",
        StreamResumeState(
            last_source_key="o1",
            last_watermark=20,
            emitted_count=1,
            scope="scope-b",
            updated_at="2026-03-13T00:00:01+00:00",
        ),
    )

    first.flush()
    second.flush()

    reloaded = ReaderCursorStore("gs://migration-state/reader-cursors.json")
    assert reloaded.get("v1:users->Users").last_source_key == "u1"
    assert reloaded.get("v1:orders->Orders").last_source_key == "o1"


def test_reader_cursor_store_roundtrip_spanner(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_spanner(monkeypatch)

    first = ReaderCursorStore(
        "spanner://proj/inst/db/MigrationControlPlane?namespace=reader-cursors"
    )
    second = ReaderCursorStore(
        "spanner://proj/inst/db/MigrationControlPlane?namespace=reader-cursors"
    )

    first.set(
        "v1:users->Users",
        StreamResumeState(
            last_source_key="u1",
            last_watermark=10,
            emitted_count=1,
            scope="scope-a",
            updated_at="2026-03-13T00:00:00+00:00",
        ),
    )
    second.set(
        "v1:users->Users",
        StreamResumeState(
            last_source_key="u2",
            last_watermark=20,
            emitted_count=2,
            scope="scope-a",
            updated_at="2026-03-13T00:00:01+00:00",
        ),
    )

    first.flush()
    second.flush()

    reloaded = ReaderCursorStore(
        "spanner://proj/inst/db/MigrationControlPlane?namespace=reader-cursors"
    )
    record = reloaded.get("v1:users->Users")
    assert record.last_source_key == "u2"
    assert record.last_watermark == 20


def test_watermark_store_roundtrip_spanner(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_spanner(monkeypatch)

    first = WatermarkStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v1-watermarks")
    second = WatermarkStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v1-watermarks")

    first.set("users", 100)
    second.set("users", 120)
    first.flush()
    second.flush()

    reloaded = WatermarkStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v1-watermarks")
    assert reloaded.get("users") == 120
