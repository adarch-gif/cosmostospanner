from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

from migration.retry_utils import RetryPolicy
from migration_v2.config import CassandraJobConfig, MongoJobConfig
from migration_v2.source_adapters.cassandra_cosmos import CassandraCosmosSourceAdapter
from migration_v2.source_adapters.mongo_cosmos import MongoCosmosSourceAdapter


class _FakeMongoCursor:
    def __init__(self, documents: list[dict[str, Any]], state: dict[str, int]) -> None:
        self._documents = documents
        self._state = state
        self._iterated = False

    def sort(self, _spec: list[tuple[str, int]]) -> "_FakeMongoCursor":
        return self

    def close(self) -> None:
        return None

    def __iter__(self):
        if not self._iterated:
            self._iterated = True
            self._state["cursor_opens"] += 1
        for index, document in enumerate(self._documents):
            if self._state["cursor_opens"] == 1 and index == 1:
                raise RuntimeError("mongo cursor dropped")
            yield document


class _FakeMongoCollection:
    def __init__(self, documents: list[dict[str, Any]], state: dict[str, int]) -> None:
        self._documents = documents
        self._state = state

    def find(self, *_args: Any, **_kwargs: Any) -> _FakeMongoCursor:
        return _FakeMongoCursor(self._documents, self._state)


class _FakeMongoDatabase:
    def __init__(self, documents: list[dict[str, Any]], state: dict[str, int]) -> None:
        self._documents = documents
        self._state = state

    def __getitem__(self, _collection_name: str) -> _FakeMongoCollection:
        return _FakeMongoCollection(self._documents, self._state)


class _FakeMongoClient:
    documents: list[dict[str, Any]] = []
    state: dict[str, int] = {"cursor_opens": 0}

    def __init__(self, _connection_string: str) -> None:
        pass

    def __getitem__(self, _database_name: str) -> _FakeMongoDatabase:
        return _FakeMongoDatabase(self.documents, self.state)

    def close(self) -> None:
        return None


class _FakeCassandraRow:
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def _asdict(self) -> dict[str, Any]:
        return dict(self._data)


class _FakeCassandraSession:
    state: dict[str, int] = {"execute_calls": 0}
    rows: list[dict[str, Any]] = []

    def set_keyspace(self, _keyspace: str) -> None:
        return None

    def prepare(self, query: str) -> str:
        return query

    def execute(self, _statement: Any, _params: Any = None):
        self.state["execute_calls"] += 1
        for index, row in enumerate(self.rows):
            if self.state["execute_calls"] == 1 and index == 1:
                raise RuntimeError("cassandra stream dropped")
            yield _FakeCassandraRow(row)

    def shutdown(self) -> None:
        return None


class _FakeCluster:
    def __init__(self, **_kwargs: Any) -> None:
        pass

    def connect(self) -> _FakeCassandraSession:
        return _FakeCassandraSession()

    def shutdown(self) -> None:
        return None


class _FakePlainTextAuthProvider:
    def __init__(self, **_kwargs: Any) -> None:
        pass


def test_mongo_adapter_resumes_after_mid_stream_failure(monkeypatch) -> None:
    pymongo_module = ModuleType("pymongo")
    pymongo_module.MongoClient = _FakeMongoClient
    monkeypatch.setitem(sys.modules, "pymongo", pymongo_module)

    _FakeMongoClient.documents = [
        {"_id": "1", "id": "1", "_ts": 10},
        {"_id": "2", "id": "2", "_ts": 11},
        {"_id": "3", "id": "3", "_ts": 12},
    ]
    _FakeMongoClient.state = {"cursor_opens": 0}

    adapter = MongoCosmosSourceAdapter(
        retry_policy=RetryPolicy(
            attempts=2,
            initial_delay_seconds=0.0,
            max_delay_seconds=0.0,
            backoff_multiplier=1.0,
            jitter_seconds=0.0,
        )
    )
    job = MongoJobConfig(
        name="mongo_users",
        api="mongodb",
        route_namespace="mongodb.appdb.users",
        key_fields=["id"],
        connection_string="mongodb://example",
        database="appdb",
        collection="users",
        incremental_field="_ts",
    )

    records = list(
        adapter.iter_records(
            job,
            mode="incremental",
            watermark=10,
            max_records=None,
        )
    )

    assert [record.source_key for record in records] == ["1", "2", "3"]
    assert _FakeMongoClient.state["cursor_opens"] == 2


def test_cassandra_adapter_resumes_after_mid_stream_failure(monkeypatch) -> None:
    cassandra_module = ModuleType("cassandra")
    cassandra_auth_module = ModuleType("cassandra.auth")
    cassandra_cluster_module = ModuleType("cassandra.cluster")
    cassandra_auth_module.PlainTextAuthProvider = _FakePlainTextAuthProvider
    cassandra_cluster_module.Cluster = _FakeCluster
    monkeypatch.setitem(sys.modules, "cassandra", cassandra_module)
    monkeypatch.setitem(sys.modules, "cassandra.auth", cassandra_auth_module)
    monkeypatch.setitem(sys.modules, "cassandra.cluster", cassandra_cluster_module)

    _FakeCassandraSession.state = {"execute_calls": 0}
    _FakeCassandraSession.rows = [
        {"order_id": "1", "updated_at": 10},
        {"order_id": "2", "updated_at": 11},
        {"order_id": "3", "updated_at": 12},
    ]

    adapter = CassandraCosmosSourceAdapter(
        retry_policy=RetryPolicy(
            attempts=2,
            initial_delay_seconds=0.0,
            max_delay_seconds=0.0,
            backoff_multiplier=1.0,
            jitter_seconds=0.0,
        )
    )
    job = CassandraJobConfig(
        name="cass_orders",
        api="cassandra",
        route_namespace="cassandra.ks.orders",
        key_fields=["order_id"],
        contact_points=["host1"],
        username="user",
        password="pass",
        keyspace="ks",
        table="orders",
        incremental_field="updated_at",
    )

    records = list(
        adapter.iter_records(
            job,
            mode="incremental",
            watermark=10,
            max_records=None,
        )
    )

    assert [record.source_key for record in records] == ["1", "2", "3"]
    assert _FakeCassandraSession.state["execute_calls"] == 2
