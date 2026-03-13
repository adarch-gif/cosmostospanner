from __future__ import annotations

import pytest

from migration.state_store import WatermarkStore


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
