from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from types import ModuleType

import pytest

from migration.coordination import (
    LeaseStore,
    PROGRESS_STATUS_COMPLETED,
    PROGRESS_STATUS_FAILED,
    PROGRESS_STATUS_RUNNING,
    WorkCoordinator,
    WorkProgressStore,
)


def _install_fake_spanner(monkeypatch: pytest.MonkeyPatch) -> None:
    commit_timestamp = object()
    store: dict[tuple[str, str, str], tuple] = {}
    tick = {"count": 0}

    class FakeKeySet:
        def __init__(self, *, keys: list[tuple[str, str]]) -> None:
            self.keys = keys

    class _BaseReader:
        def read(self, *, table: str, columns: list[str], keyset: FakeKeySet):
            del columns
            rows: list[tuple] = []
            for namespace, record_key in keyset.keys:
                row = store.get((table, namespace, record_key))
                if row is not None:
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

    spanner_module.Client = FakeSpannerClient
    spanner_module.COMMIT_TIMESTAMP = commit_timestamp
    spanner_v1_module.KeySet = FakeKeySet
    cloud_module.spanner = spanner_module
    cloud_module.spanner_v1 = spanner_v1_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.spanner", spanner_module)
    monkeypatch.setitem(sys.modules, "google.cloud.spanner_v1", spanner_v1_module)


def test_lease_store_acquire_renew_and_release(tmp_path) -> None:
    lease_store = LeaseStore(str(tmp_path / "leases.json"))
    now = datetime(2026, 3, 13, tzinfo=UTC)

    assert lease_store.try_acquire(
        "mapping-a",
        owner_id="worker-a",
        lease_duration_seconds=60,
        metadata={"kind": "mapping"},
        now=now,
    )
    assert not lease_store.try_acquire(
        "mapping-a",
        owner_id="worker-b",
        lease_duration_seconds=60,
        now=now + timedelta(seconds=10),
    )

    assert lease_store.renew(
        "mapping-a",
        owner_id="worker-a",
        lease_duration_seconds=60,
        now=now + timedelta(seconds=20),
    )
    record = lease_store.get("mapping-a")
    assert record is not None
    assert record.owner_id == "worker-a"
    assert record.metadata == {"kind": "mapping"}

    assert lease_store.release("mapping-a", owner_id="worker-a")
    assert lease_store.get("mapping-a") is None


def test_lease_store_allows_expired_lease_to_be_stolen(tmp_path) -> None:
    lease_store = LeaseStore(str(tmp_path / "leases.json"))
    now = datetime(2026, 3, 13, tzinfo=UTC)

    assert lease_store.try_acquire(
        "job-a",
        owner_id="worker-a",
        lease_duration_seconds=5,
        now=now,
    )
    assert lease_store.try_acquire(
        "job-a",
        owner_id="worker-b",
        lease_duration_seconds=30,
        now=now + timedelta(seconds=6),
    )

    record = lease_store.get("job-a")
    assert record is not None
    assert record.owner_id == "worker-b"


def test_work_coordinator_renews_only_when_due(tmp_path) -> None:
    coordinator = WorkCoordinator(
        lease_file=str(tmp_path / "leases.json"),
        worker_id="worker-a",
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
    )

    assert coordinator.acquire("job-a", metadata={"job": "a"})
    assert coordinator.renew_if_due("job-a", metadata={"job": "a"})
    assert coordinator.release("job-a")


def test_work_progress_store_tracks_running_completed_and_failed_states(tmp_path) -> None:
    progress_store = WorkProgressStore(str(tmp_path / "progress.json"))

    progress_store.mark_running("run-1:v1:users->Users:shard=0", owner_id="worker-a")
    running = progress_store.get("run-1:v1:users->Users:shard=0")
    assert running is not None
    assert running.status == PROGRESS_STATUS_RUNNING
    assert running.attempt_count == 1

    progress_store.mark_failed(
        "run-1:v1:users->Users:shard=0",
        owner_id="worker-a",
        error="transient failure",
    )
    failed = progress_store.get("run-1:v1:users->Users:shard=0")
    assert failed is not None
    assert failed.status == PROGRESS_STATUS_FAILED
    assert failed.last_error == "transient failure"

    progress_store.mark_running("run-1:v1:users->Users:shard=0", owner_id="worker-b")
    retried = progress_store.get("run-1:v1:users->Users:shard=0")
    assert retried is not None
    assert retried.status == PROGRESS_STATUS_RUNNING
    assert retried.attempt_count == 2

    progress_store.mark_completed("run-1:v1:users->Users:shard=0", owner_id="worker-b")
    completed = progress_store.get("run-1:v1:users->Users:shard=0")
    assert completed is not None
    assert completed.status == PROGRESS_STATUS_COMPLETED
    assert completed.attempt_count == 2


def test_work_coordinator_reports_completed_work_for_run_id(tmp_path) -> None:
    coordinator = WorkCoordinator(
        lease_file=str(tmp_path / "leases.json"),
        progress_file=str(tmp_path / "progress.json"),
        run_id="full-20260313",
        worker_id="worker-a",
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
    )

    coordinator.mark_running("v2:mongo_users:shard=1", metadata={"job": "mongo_users"})
    assert not coordinator.is_completed("v2:mongo_users:shard=1")
    coordinator.mark_completed("v2:mongo_users:shard=1", metadata={"job": "mongo_users"})
    assert coordinator.is_completed("v2:mongo_users:shard=1")


def test_work_coordinator_claim_next_skips_completed_manifest_entries(tmp_path) -> None:
    coordinator = WorkCoordinator(
        lease_file=str(tmp_path / "leases.json"),
        progress_file=str(tmp_path / "progress.json"),
        run_id="full-20260313",
        worker_id="worker-a",
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
    )
    coordinator.mark_completed("v1:users->Users:shard=0", metadata={"mapping": "users"})

    claimed = coordinator.claim_next(
        {
            "v1:users->Users:shard=0": {"mapping": "users", "shard_index": 0},
            "v1:users->Users:shard=1": {"mapping": "users", "shard_index": 1},
        }
    )

    assert claimed is not None
    assert claimed[0] == "v1:users->Users:shard=1"


def test_work_coordinator_reclaims_stale_running_shards(tmp_path) -> None:
    coordinator = WorkCoordinator(
        lease_file=str(tmp_path / "leases.json"),
        progress_file=str(tmp_path / "progress.json"),
        run_id="full-20260313",
        worker_id="worker-b",
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
    )
    lease_store = LeaseStore(str(tmp_path / "leases.json"))
    progress_store = WorkProgressStore(str(tmp_path / "progress.json"))
    now = datetime(2026, 3, 13, tzinfo=UTC)

    assert lease_store.try_acquire(
        "v1:users->Users:shard=0",
        owner_id="worker-a",
        lease_duration_seconds=5,
        metadata={"mapping": "users"},
        now=now,
    )
    progress_store.mark_running(
        "full-20260313:v1:users->Users:shard=0",
        owner_id="worker-a",
        metadata={"mapping": "users"},
        now=now,
    )

    reclaimed = coordinator.reclaim_stale_running(
        {"v1:users->Users:shard=0": {"mapping": "users"}},
        now=now + timedelta(seconds=6),
    )

    assert reclaimed == ["v1:users->Users:shard=0"]
    progress = coordinator.progress("v1:users->Users:shard=0")
    assert progress is not None
    assert progress.status == PROGRESS_STATUS_FAILED
    assert "stale lease recovered" in progress.last_error


def test_lease_store_roundtrip_spanner(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_spanner(monkeypatch)
    lease_store = LeaseStore(
        "spanner://proj/inst/db/MigrationControlPlane?namespace=leases"
    )

    assert lease_store.try_acquire(
        "mapping-a",
        owner_id="worker-a",
        lease_duration_seconds=60,
        metadata={"kind": "mapping"},
        now=datetime(2026, 3, 13, tzinfo=UTC),
    )
    record = lease_store.get("mapping-a")
    assert record is not None
    assert record.owner_id == "worker-a"
    assert record.metadata == {"kind": "mapping"}


def test_work_coordinator_claim_next_roundtrips_through_spanner_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_spanner(monkeypatch)
    coordinator = WorkCoordinator(
        lease_file="spanner://proj/inst/db/MigrationControlPlane?namespace=leases",
        progress_file="spanner://proj/inst/db/MigrationControlPlane?namespace=progress",
        run_id="full-20260313",
        worker_id="worker-a",
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
    )
    coordinator.mark_completed("v1:users->Users:shard=0", metadata={"mapping": "users"})

    claimed = coordinator.claim_next(
        {
            "v1:users->Users:shard=0": {"mapping": "users", "shard_index": 0},
            "v1:users->Users:shard=1": {"mapping": "users", "shard_index": 1},
        }
    )

    assert claimed is not None
    assert claimed[0] == "v1:users->Users:shard=1"
    progress = coordinator.progress("v1:users->Users:shard=1")
    lease = coordinator._store.get("v1:users->Users:shard=1") if coordinator._store else None
    assert progress is not None
    assert progress.status == PROGRESS_STATUS_RUNNING
    assert lease is not None
    assert lease.owner_id == "worker-a"
