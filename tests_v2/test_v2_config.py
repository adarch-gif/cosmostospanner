from __future__ import annotations

import yaml

from migration_v2.config import load_v2_config


def _write_config(tmp_path, data: dict) -> str:
    path = tmp_path / "v2.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def test_load_v2_config_parses_mongo_and_cassandra(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    monkeypatch.setenv("COSMOS_CASSANDRA_USERNAME", "user")
    monkeypatch.setenv("COSMOS_CASSANDRA_PASSWORD", "pass")
    data = {
        "runtime": {"mode": "incremental", "batch_size": 100},
        "routing": {"firestore_lt_bytes": 1_048_576},
        "targets": {
            "firestore": {"project": "proj", "collection": "coll"},
            "spanner": {
                "project": "proj",
                "instance": "inst",
                "database": "db",
                "table": "RoutedDocuments",
            },
        },
        "jobs": [
            {
                "name": "mongo_users",
                "api": "mongodb",
                "connection_string_env": "COSMOS_MONGO_CONNECTION_STRING",
                "database": "appdb",
                "collection": "users",
                "route_namespace": "mongodb.appdb.users",
                "key_fields": ["id"],
            },
            {
                "name": "cass_orders",
                "api": "cassandra",
                "contact_points": ["host1"],
                "username_env": "COSMOS_CASSANDRA_USERNAME",
                "password_env": "COSMOS_CASSANDRA_PASSWORD",
                "keyspace": "ks",
                "table": "orders",
                "route_namespace": "cassandra.ks.orders",
                "key_fields": ["order_id"],
            },
        ],
    }

    cfg = load_v2_config(_write_config(tmp_path, data))
    assert cfg.runtime.mode == "incremental"
    assert len(cfg.jobs) == 2
    assert cfg.jobs[0].api == "mongodb"
    assert cfg.jobs[1].api == "cassandra"

