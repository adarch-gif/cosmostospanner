from __future__ import annotations

from datetime import datetime, timezone

import pytest

from migration.config import ColumnRule, DeleteRule, TableMapping
from migration.transform import transform_document


def _mapping() -> TableMapping:
    return TableMapping(
        source_container="users",
        target_table="Users",
        key_columns=["user_id"],
        columns=[
            ColumnRule(target="user_id", source="id", converter="string", required=True),
            ColumnRule(target="email", source="profile.email", converter="string"),
            ColumnRule(target="active", source="flags.active", converter="bool"),
            ColumnRule(target="first_city", source="addresses.0.city", converter="string"),
            ColumnRule(target="profile_json", source="profile", converter="json_string"),
        ],
        static_columns={"migrated_at": "__NOW_UTC__"},
        delete_rule=DeleteRule(field="isDeleted", equals=True),
    )


def test_transform_document_happy_path() -> None:
    mapping = _mapping()
    doc = {
        "id": "u1",
        "_ts": 1700000000,
        "profile": {"email": "u1@example.com"},
        "flags": {"active": "true"},
        "addresses": [{"city": "New York"}],
        "isDeleted": False,
    }
    result = transform_document(doc, mapping)
    assert result.row["user_id"] == "u1"
    assert result.row["email"] == "u1@example.com"
    assert result.row["active"] is True
    assert result.row["first_city"] == "New York"
    assert isinstance(result.row["migrated_at"], datetime)
    assert result.row["migrated_at"].tzinfo is not None
    assert result.source_ts == 1700000000
    assert result.is_delete is False


def test_transform_document_handles_delete_rule() -> None:
    mapping = _mapping()
    doc = {"id": "u2", "profile": {"email": "u2@example.com"}, "isDeleted": True}
    result = transform_document(doc, mapping)
    assert result.is_delete is True


def test_transform_document_requires_key() -> None:
    mapping = _mapping()
    doc = {"profile": {"email": "u3@example.com"}}
    with pytest.raises(ValueError, match="Missing required source field|Key column"):
        transform_document(doc, mapping)


def test_transform_document_parses_timestamp_converter() -> None:
    mapping = TableMapping(
        source_container="orders",
        target_table="Orders",
        key_columns=["order_id"],
        columns=[
            ColumnRule(target="order_id", source="id", converter="string", required=True),
            ColumnRule(target="created_at", source="createdAt", converter="timestamp"),
        ],
    )
    doc = {"id": "o1", "createdAt": "2026-03-12T12:00:00Z"}
    result = transform_document(doc, mapping)
    created_at = result.row["created_at"]
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is not None
    assert created_at.astimezone(timezone.utc).isoformat().startswith("2026-03-12T12:00:00")
