from __future__ import annotations

from datetime import datetime, timezone

from migration.reconciliation import (
    SqliteRowDigestStore,
    aggregate_row_digests,
    row_digest,
)


def test_row_digest_normalizes_datetime_values() -> None:
    row = {
        "user_id": "u1",
        "created_at": datetime(2026, 3, 12, 12, 0, tzinfo=timezone.utc),
    }
    digest_a = row_digest(row, ["user_id", "created_at"])
    digest_b = row_digest(
        {
            "user_id": "u1",
            "created_at": "2026-03-12T12:00:00+00:00",
        },
        ["user_id", "created_at"],
    )
    assert digest_a == digest_b


def test_aggregate_row_digests_is_order_independent() -> None:
    left = {
        ("u1",): "a" * 64,
        ("u2",): "b" * 64,
    }
    right = {
        ("u2",): "b" * 64,
        ("u1",): "a" * 64,
    }
    assert aggregate_row_digests(left) == aggregate_row_digests(right)


def test_sqlite_row_digest_store_matches_dict_aggregation() -> None:
    source = {
        ("u1",): "a" * 64,
        ("u2",): "b" * 64,
    }
    target = {
        ("u2",): "b" * 64,
        ("u1",): "a" * 64,
    }

    with SqliteRowDigestStore() as store:
        for key_tuple, digest in source.items():
            store.upsert_source(key_tuple, digest)
        for key_tuple, digest in target.items():
            store.upsert_target(key_tuple, digest)

        summary = store.summarize()

    assert summary.source_count == 2
    assert summary.target_count == 2
    assert summary.missing_rows == 0
    assert summary.extra_rows == 0
    assert summary.mismatched_rows == 0
    assert summary.source_checksum == aggregate_row_digests(source)
    assert summary.target_checksum == aggregate_row_digests(target)


def test_sqlite_row_digest_store_reports_missing_extra_and_mismatched_rows() -> None:
    with SqliteRowDigestStore() as store:
        store.upsert_source(("u1",), "a" * 64)
        store.upsert_source(("u2",), "b" * 64)
        store.upsert_target(("u2",), "c" * 64)
        store.upsert_target(("u3",), "d" * 64)

        summary = store.summarize()

    assert summary.source_count == 2
    assert summary.target_count == 2
    assert summary.missing_rows == 1
    assert summary.extra_rows == 1
    assert summary.mismatched_rows == 1
