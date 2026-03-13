from __future__ import annotations

import sys
from types import ModuleType

import pytest

from migration.state_store import WatermarkStore


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
