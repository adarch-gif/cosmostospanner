from __future__ import annotations

import re
import sys
from datetime import UTC, datetime, timedelta
from types import ModuleType

import pytest

from migration_v2.state_store import (
    RouteRegistryEntry,
    RouteRegistryStore,
    WatermarkCheckpoint,
    WatermarkStore,
)


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


def test_route_registry_store_roundtrip_with_sync_state(tmp_path) -> None:
    path = tmp_path / "registry.json"
    store = RouteRegistryStore(str(path))
    store.set_entry(
        "ns|1",
        RouteRegistryEntry(
            destination="spanner",
            checksum="abc",
            payload_size_bytes=200,
            updated_at="2026-03-12T00:00:00+00:00",
            sync_state="pending_cleanup",
            cleanup_from_destination="firestore",
        ),
    )
    store.flush()

    reloaded = RouteRegistryStore(str(path))
    entry = reloaded.get_entry("ns|1")
    assert entry is not None
    assert entry.sync_state == "pending_cleanup"
    assert entry.cleanup_from_destination == "firestore"


def test_route_registry_store_rejects_corrupted_json(tmp_path) -> None:
    path = tmp_path / "registry.json"
    path.write_text("{bad json", encoding="utf-8")
    with pytest.raises(ValueError, match="corrupted"):
        RouteRegistryStore(str(path))


def test_v2_watermark_store_roundtrip_checkpoint(tmp_path) -> None:
    path = tmp_path / "watermarks.json"
    store = WatermarkStore(str(path))
    store.set_checkpoint(
        "mongo_users",
        WatermarkCheckpoint(
            watermark=100,
            route_keys=["job|1", "job|2"],
            updated_at="2026-03-12T00:00:00+00:00",
        ),
    )
    store.flush()

    reloaded = WatermarkStore(str(path))
    checkpoint = reloaded.get_checkpoint("mongo_users")
    assert checkpoint.watermark == 100
    assert checkpoint.route_keys == ["job|1", "job|2"]


def test_v2_watermark_store_roundtrip_checkpoint_spanner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_spanner(monkeypatch)

    first = WatermarkStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v2-watermarks")
    second = WatermarkStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v2-watermarks")

    first.set_checkpoint(
        "mongo_users",
        WatermarkCheckpoint(
            watermark=100,
            route_keys=["job|1"],
            updated_at="2026-03-12T00:00:00+00:00",
        ),
    )
    second.set_checkpoint(
        "mongo_users",
        WatermarkCheckpoint(
            watermark=100,
            route_keys=["job|2"],
            updated_at="2026-03-12T00:00:01+00:00",
        ),
    )
    first.flush()
    second.flush()

    reloaded = WatermarkStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v2-watermarks")
    checkpoint = reloaded.get_checkpoint("mongo_users")
    assert checkpoint.watermark == 100
    assert checkpoint.route_keys == ["job|1", "job|2"]


def test_route_registry_store_items_spanner(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_spanner(monkeypatch)

    store = RouteRegistryStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v2-registry")
    store.set_entry(
        "ns|2",
        RouteRegistryEntry(
            destination="firestore",
            checksum="b",
            payload_size_bytes=10,
            updated_at="2026-03-12T00:00:01+00:00",
        ),
    )
    store.set_entry(
        "ns|1",
        RouteRegistryEntry(
            destination="spanner",
            checksum="a",
            payload_size_bytes=20,
            updated_at="2026-03-12T00:00:00+00:00",
            sync_state="pending_cleanup",
            cleanup_from_destination="firestore",
        ),
    )
    store.flush()

    reloaded = RouteRegistryStore("spanner://proj/inst/db/MigrationControlPlane?namespace=v2-registry")
    items = reloaded.items()
    assert [key for key, _ in items] == ["ns|1", "ns|2"]
    assert items[0][1].sync_state == "pending_cleanup"
