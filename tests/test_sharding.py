from __future__ import annotations

from migration.sharding import (
    SHARD_COUNT_PLACEHOLDER,
    SHARD_INDEX_PLACEHOLDER,
    apply_shard_placeholders,
    contains_shard_placeholders,
    shard_execution_order,
    stable_shard_for_text,
)


def test_apply_shard_placeholders_replaces_supported_tokens() -> None:
    rendered = apply_shard_placeholders(
        f"SELECT * FROM c WHERE MOD(c.partitionKey, {SHARD_COUNT_PLACEHOLDER}) = {SHARD_INDEX_PLACEHOLDER}",
        shard_index=2,
        shard_count=8,
    )

    assert rendered == "SELECT * FROM c WHERE MOD(c.partitionKey, 8) = 2"


def test_contains_shard_placeholders_detects_supported_tokens() -> None:
    assert contains_shard_placeholders(f"{{\"shard\": \"{SHARD_INDEX_PLACEHOLDER}\"}}")
    assert contains_shard_placeholders(f"count={SHARD_COUNT_PLACEHOLDER}")
    assert not contains_shard_placeholders("SELECT * FROM c")


def test_stable_shard_for_text_is_deterministic() -> None:
    assert stable_shard_for_text("tenant-42", 16) == stable_shard_for_text("tenant-42", 16)


def test_shard_execution_order_rotates_by_worker() -> None:
    order = shard_execution_order("v1:users->Users", "worker-a", 4)

    assert sorted(order) == [0, 1, 2, 3]
    assert len(set(order)) == 4
