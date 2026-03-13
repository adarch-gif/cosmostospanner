from __future__ import annotations

import hashlib

SHARD_INDEX_PLACEHOLDER = "{{SHARD_INDEX}}"
SHARD_COUNT_PLACEHOLDER = "{{SHARD_COUNT}}"


def stable_hash_int(text: str) -> int:
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest(), 16)


def stable_shard_for_text(text: str, shard_count: int) -> int:
    if shard_count <= 0:
        raise ValueError("shard_count must be > 0")
    return stable_hash_int(text) % shard_count


def apply_shard_placeholders(template: str, *, shard_index: int, shard_count: int) -> str:
    return (
        template.replace(SHARD_INDEX_PLACEHOLDER, str(shard_index))
        .replace(SHARD_COUNT_PLACEHOLDER, str(shard_count))
    )


def contains_shard_placeholders(template: str) -> bool:
    return SHARD_INDEX_PLACEHOLDER in template or SHARD_COUNT_PLACEHOLDER in template


def shard_execution_order(namespace: str, worker_id: str, shard_count: int) -> list[int]:
    if shard_count <= 0:
        return []
    offset = stable_hash_int(f"{namespace}|{worker_id}") % shard_count
    return [((offset + index) % shard_count) for index in range(shard_count)]
