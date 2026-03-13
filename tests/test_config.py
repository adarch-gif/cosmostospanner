from __future__ import annotations

from copy import deepcopy

import pytest
import yaml

from migration.config import load_config


def _base_config() -> dict:
    return {
        "source": {
            "endpoint": "https://example.documents.azure.com:443/",
            "key": "test-key",
            "database": "srcdb",
        },
        "target": {
            "project": "proj",
            "instance": "inst",
            "database": "tgtdb",
        },
        "runtime": {
            "batch_size": 200,
            "query_page_size": 100,
        },
        "mappings": [
            {
                "source_container": "users",
                "target_table": "Users",
                "key_columns": ["user_id"],
                "columns": [
                    {
                        "target": "user_id",
                        "source": "id",
                        "converter": "string",
                        "required": True,
                    },
                    {
                        "target": "email",
                        "source": "profile.email",
                        "converter": "string",
                    },
                ],
                "static_columns": {"tenant_id": "acme"},
            }
        ],
    }


def _write_config(tmp_path, data: dict) -> str:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(config_path)


def test_load_config_parses_runtime_booleans_and_validation_columns(tmp_path) -> None:
    cfg = _base_config()
    cfg["runtime"].update(
        {
            "dry_run": "true",
            "flush_watermark_each_mapping": "yes",
            "retry_attempts": 3,
            "retry_initial_delay_seconds": 0.1,
            "retry_max_delay_seconds": 2.0,
            "retry_backoff_multiplier": 2.0,
            "retry_jitter_seconds": 0.0,
        }
    )
    cfg["mappings"][0]["validation_columns"] = ["email"]
    config = load_config(_write_config(tmp_path, cfg))
    assert config.runtime.dry_run is True
    assert config.runtime.flush_watermark_each_mapping is True
    assert config.runtime.retry_attempts == 3
    assert config.mappings[0].validation_columns == ["email"]


def test_load_config_rejects_duplicate_target_columns(tmp_path) -> None:
    cfg = _base_config()
    cfg["mappings"][0]["columns"].append(
        {
            "target": "email",
            "source": "altEmail",
            "converter": "string",
        }
    )
    with pytest.raises(ValueError, match="Duplicate target column"):
        load_config(_write_config(tmp_path, cfg))


def test_load_config_rejects_key_columns_not_in_output(tmp_path) -> None:
    cfg = _base_config()
    cfg["mappings"][0]["key_columns"] = ["missing_key"]
    with pytest.raises(ValueError, match="Key columns .* are not produced"):
        load_config(_write_config(tmp_path, cfg))


def test_load_config_rejects_bad_retry_settings(tmp_path) -> None:
    cfg = _base_config()
    bad = deepcopy(cfg)
    bad["runtime"]["retry_attempts"] = 0
    with pytest.raises(ValueError, match="retry_attempts"):
        load_config(_write_config(tmp_path, bad))


def test_load_config_rejects_invalid_spanner_identifiers(tmp_path) -> None:
    cfg = _base_config()
    cfg["mappings"][0]["target_table"] = "Users;DROP TABLE X"
    with pytest.raises(ValueError, match="valid Spanner identifier"):
        load_config(_write_config(tmp_path, cfg))
