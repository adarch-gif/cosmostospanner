from __future__ import annotations

from datetime import datetime, timezone

from migration.reconciliation import aggregate_row_digests, row_digest


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
