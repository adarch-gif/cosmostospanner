from __future__ import annotations

import pytest
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
        "runtime": {
            "mode": "incremental",
            "batch_size": 100,
            "lease_file": "gs://bucket/v2-leases.json",
            "reader_cursor_state_file": "gs://bucket/v2-reader-cursors.json",
            "worker_id": "worker-1",
            "lease_duration_seconds": 120,
            "heartbeat_interval_seconds": 30,
            "deployment_environment": "prod",
            "release_gate_file": "gs://bucket/release-gate.json",
            "release_gate_scope": "cutover-20260313",
            "release_gate_max_age_hours": 48,
            "require_stage_rehearsal_for_prod": True,
        },
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
    assert cfg.runtime.lease_file == "gs://bucket/v2-leases.json"
    assert cfg.runtime.reader_cursor_state_file == "gs://bucket/v2-reader-cursors.json"
    assert cfg.runtime.worker_id == "worker-1"
    assert cfg.runtime.deployment_environment == "prod"
    assert cfg.runtime.release_gate_file == "gs://bucket/release-gate.json"
    assert cfg.runtime.release_gate_scope == "cutover-20260313"
    assert cfg.runtime.release_gate_max_age_hours == 48
    assert cfg.runtime.require_stage_rehearsal_for_prod is True
    assert len(cfg.jobs) == 2
    assert cfg.jobs[0].api == "mongodb"
    assert cfg.jobs[1].api == "cassandra"


def test_load_v2_config_rejects_invalid_spanner_table(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    data = {
        "runtime": {"mode": "full"},
        "routing": {"firestore_lt_bytes": 1_048_576},
        "targets": {
            "firestore": {"project": "proj"},
            "spanner": {
                "project": "proj",
                "instance": "inst",
                "database": "db",
                "table": "RoutedDocuments;DROP",
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
            }
        ],
    }
    with pytest.raises(ValueError, match="valid Spanner identifier"):
        load_v2_config(_write_config(tmp_path, data))


def test_load_v2_config_rejects_unsafe_spanner_payload_limit(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    data = {
        "runtime": {"mode": "full"},
        "routing": {
            "firestore_lt_bytes": 1_048_576,
            "spanner_max_payload_bytes": 10_485_760,
        },
        "targets": {
            "firestore": {"project": "proj"},
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
            }
        ],
    }
    with pytest.raises(ValueError, match="safe recommended limit"):
        load_v2_config(_write_config(tmp_path, data))


def test_load_v2_config_rejects_invalid_lease_timing(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    data = {
        "runtime": {
            "mode": "full",
            "lease_file": "gs://bucket/v2-leases.json",
            "lease_duration_seconds": 30,
            "heartbeat_interval_seconds": 30,
        },
        "routing": {"firestore_lt_bytes": 1_048_576},
        "targets": {
            "firestore": {"project": "proj"},
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
            }
        ],
    }
    with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
        load_v2_config(_write_config(tmp_path, data))


def test_load_v2_config_rejects_progress_file_without_run_id(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    data = {
        "runtime": {
            "mode": "full",
            "progress_file": "gs://bucket/v2-progress.json",
        },
        "routing": {"firestore_lt_bytes": 1_048_576},
        "targets": {
            "firestore": {"project": "proj"},
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
            }
        ],
    }
    with pytest.raises(ValueError, match="runtime.run_id"):
        load_v2_config(_write_config(tmp_path, data))


def test_load_v2_config_rejects_release_gate_file_without_scope(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    data = {
        "runtime": {
            "mode": "full",
            "release_gate_file": "gs://bucket/release-gate.json",
        },
        "routing": {"firestore_lt_bytes": 1_048_576},
        "targets": {
            "firestore": {"project": "proj"},
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
            }
        ],
    }
    with pytest.raises(ValueError, match="runtime.release_gate_scope"):
        load_v2_config(_write_config(tmp_path, data))


def test_load_v2_config_rejects_prod_release_gate_requirement_without_file(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    data = {
        "runtime": {
            "mode": "full",
            "deployment_environment": "prod",
            "release_gate_scope": "cutover-1",
            "require_stage_rehearsal_for_prod": True,
        },
        "routing": {"firestore_lt_bytes": 1_048_576},
        "targets": {
            "firestore": {"project": "proj"},
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
            }
        ],
    }
    with pytest.raises(ValueError, match="runtime.release_gate_file"):
        load_v2_config(_write_config(tmp_path, data))


def test_load_v2_config_rejects_query_template_sharding_without_placeholders(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("COSMOS_MONGO_CONNECTION_STRING", "mongodb://example")
    data = {
        "runtime": {"mode": "full"},
        "routing": {"firestore_lt_bytes": 1_048_576},
        "targets": {
            "firestore": {"project": "proj"},
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
                "shard_count": 4,
                "shard_mode": "query_template",
                "source_query": "{}",
            }
        ],
    }
    with pytest.raises(ValueError, match="shard placeholders"):
        load_v2_config(_write_config(tmp_path, data))
