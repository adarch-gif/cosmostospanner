from __future__ import annotations

import pytest

from migration_v2.state_store import (
    RouteRegistryEntry,
    RouteRegistryStore,
    WatermarkCheckpoint,
    WatermarkStore,
)


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
