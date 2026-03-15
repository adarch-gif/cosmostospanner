from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from migration.sharding import contains_shard_placeholders

SPANNER_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


@dataclass
class CosmosConfig:
    endpoint: str
    key: str
    database: str


@dataclass
class SpannerConfig:
    project: str
    instance: str
    database: str


@dataclass
class RuntimeConfig:
    deployment_environment: str = "dev"
    batch_size: int = 500
    query_page_size: int = 200
    dry_run: bool = False
    log_level: str = "INFO"
    log_format: str = "text"
    watermark_state_file: str = "state/watermarks.json"
    watermark_overlap_seconds: int = 5
    flush_watermark_each_mapping: bool = True
    error_mode: str = "fail"  # fail | skip
    dlq_file_path: str = "state/dead_letter.jsonl"
    metrics_file_path: str = ""
    metrics_format: str = "prometheus"
    retry_attempts: int = 5
    retry_initial_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 15.0
    retry_backoff_multiplier: float = 2.0
    retry_jitter_seconds: float = 0.25
    max_docs_per_container: dict[str, int | None] = field(default_factory=dict)
    lease_file: str = ""
    progress_file: str = ""
    run_id: str = ""
    worker_id: str = ""
    lease_duration_seconds: int = 120
    heartbeat_interval_seconds: int = 30
    reader_cursor_state_file: str = ""
    release_gate_file: str = ""
    release_gate_scope: str = ""
    release_gate_max_age_hours: int = 72
    require_stage_rehearsal_for_prod: bool = False


@dataclass
class ColumnRule:
    target: str
    source: str | None = None
    converter: str = "identity"
    default: Any = None
    required: bool = False


@dataclass
class DeleteRule:
    field: str
    equals: Any = True


@dataclass
class TableMapping:
    source_container: str
    target_table: str
    key_columns: list[str]
    columns: list[ColumnRule]
    mode: str = "upsert"  # upsert | insert | update | replace
    source_query: str = "SELECT * FROM c"
    incremental_query: str | None = None
    static_columns: dict[str, Any] = field(default_factory=dict)
    delete_rule: DeleteRule | None = None
    validation_columns: list[str] = field(default_factory=list)
    shard_count: int = 1
    shard_mode: str = "none"  # none | client_hash | query_template
    shard_key_source: str | None = None


@dataclass
class MigrationConfig:
    source: CosmosConfig
    target: SpannerConfig
    runtime: RuntimeConfig
    mappings: list[TableMapping]


def _require(raw: dict[str, Any], key: str) -> Any:
    value = raw.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required config key: {key}")
    return value


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Cannot parse boolean value: {value!r}")


def _parse_string_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list of strings.")
    parsed: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(f"{field_name} must contain non-empty strings.")
        parsed.append(item)
    return parsed


def _validate_spanner_identifier(name: str, field_name: str) -> None:
    if not SPANNER_IDENTIFIER_RE.fullmatch(name):
        raise ValueError(
            f"{field_name} must be a valid Spanner identifier "
            "(start with letter, followed by letters/numbers/_). "
            f"Received: {name!r}"
        )


def _parse_column_rules(raw_columns: Any) -> list[ColumnRule]:
    rules: list[ColumnRule] = []
    if isinstance(raw_columns, list):
        for entry in raw_columns:
            rules.append(
                ColumnRule(
                    target=_require(entry, "target"),
                    source=entry.get("source"),
                    converter=entry.get("converter", "identity"),
                    default=entry.get("default"),
                    required=_parse_bool(entry.get("required", False), default=False),
                )
            )
        _validate_column_targets(rules)
        return rules

    if isinstance(raw_columns, dict):
        for target, value in raw_columns.items():
            if isinstance(value, str):
                rules.append(ColumnRule(target=target, source=value))
            elif isinstance(value, dict):
                rules.append(
                    ColumnRule(
                        target=target,
                        source=value.get("source"),
                        converter=value.get("converter", "identity"),
                        default=value.get("default"),
                        required=_parse_bool(value.get("required", False), default=False),
                    )
                )
            else:
                raise ValueError(f"Unsupported column mapping for {target}: {value!r}")
        _validate_column_targets(rules)
        return rules

    raise ValueError("`columns` must be a list or dictionary.")


def _validate_column_targets(rules: list[ColumnRule]) -> None:
    seen: set[str] = set()
    for rule in rules:
        _validate_spanner_identifier(rule.target, "columns[].target")
        if rule.target in seen:
            raise ValueError(f"Duplicate target column in mapping: {rule.target}")
        seen.add(rule.target)


def _parse_mapping(raw: dict[str, Any]) -> TableMapping:
    delete_rule_raw = raw.get("delete_rule")
    delete_rule = None
    if delete_rule_raw:
        delete_rule = DeleteRule(
            field=_require(delete_rule_raw, "field"),
            equals=delete_rule_raw.get("equals", True),
        )

    key_columns_raw = _require(raw, "key_columns")
    if not isinstance(key_columns_raw, list) or not key_columns_raw:
        raise ValueError(
            f"Mapping {_require(raw, 'source_container')}->{_require(raw, 'target_table')} "
            "`key_columns` must be a non-empty list."
        )
    if any(not isinstance(item, str) or not item for item in key_columns_raw):
        raise ValueError(
            f"Mapping {_require(raw, 'source_container')}->{_require(raw, 'target_table')} "
            "`key_columns` must contain non-empty strings."
        )

    columns = _parse_column_rules(_require(raw, "columns"))
    static_columns = dict(raw.get("static_columns", {}))
    validation_columns = _parse_string_list(raw.get("validation_columns"), "validation_columns")
    mapping = TableMapping(
        source_container=_require(raw, "source_container"),
        target_table=_require(raw, "target_table"),
        key_columns=list(key_columns_raw),
        columns=columns,
        mode=str(raw.get("mode", "upsert")).lower(),
        source_query=raw.get("source_query", "SELECT * FROM c"),
        incremental_query=raw.get("incremental_query"),
        static_columns=static_columns,
        delete_rule=delete_rule,
        validation_columns=validation_columns,
        shard_count=int(raw.get("shard_count", 1)),
        shard_mode=str(raw.get("shard_mode", "none")).lower(),
        shard_key_source=raw.get("shard_key_source"),
    )
    _validate_spanner_identifier(mapping.target_table, "target_table")
    if mapping.delete_rule and not mapping.key_columns:
        raise ValueError(
            f"Mapping {mapping.source_container}->{mapping.target_table} uses delete_rule "
            "but has no key_columns."
        )
    if mapping.mode not in {"upsert", "insert", "update", "replace"}:
        raise ValueError(
            f"Mapping mode {mapping.mode!r} is invalid for "
            f"{mapping.source_container}->{mapping.target_table}."
        )

    available_targets = {rule.target for rule in mapping.columns}
    available_targets.update(mapping.static_columns.keys())
    for key_column in mapping.key_columns:
        _validate_spanner_identifier(key_column, "key_columns[]")
    for column_name in mapping.static_columns:
        _validate_spanner_identifier(column_name, "static_columns key")
    missing_key_targets = [k for k in mapping.key_columns if k not in available_targets]
    if missing_key_targets:
        raise ValueError(
            f"Key columns {missing_key_targets} are not produced by columns/static_columns "
            f"in mapping {mapping.source_container}->{mapping.target_table}."
        )
    if len(mapping.validation_columns) != len(set(mapping.validation_columns)):
        raise ValueError(
            f"validation_columns has duplicates in mapping "
            f"{mapping.source_container}->{mapping.target_table}."
        )
    for validation_column in mapping.validation_columns:
        _validate_spanner_identifier(validation_column, "validation_columns[]")
    invalid_validation_columns = [
        col for col in mapping.validation_columns if col not in available_targets
    ]
    if invalid_validation_columns:
        raise ValueError(
            f"validation_columns {invalid_validation_columns} are not mapped output columns "
            f"in mapping {mapping.source_container}->{mapping.target_table}."
        )
    if mapping.shard_count <= 0:
        raise ValueError(
            f"shard_count must be > 0 in mapping {mapping.source_container}->{mapping.target_table}."
        )
    if mapping.shard_mode not in {"none", "client_hash", "query_template"}:
        raise ValueError(
            f"shard_mode {mapping.shard_mode!r} is invalid in mapping "
            f"{mapping.source_container}->{mapping.target_table}."
        )
    if mapping.shard_count > 1 and mapping.shard_mode == "none":
        raise ValueError(
            f"Mapping {mapping.source_container}->{mapping.target_table} sets shard_count > 1 "
            "but shard_mode is not configured."
        )
    if mapping.shard_mode == "client_hash" and mapping.shard_count > 1 and not mapping.shard_key_source:
        raise ValueError(
            f"Mapping {mapping.source_container}->{mapping.target_table} uses shard_mode=client_hash "
            "but shard_key_source is missing."
        )
    if mapping.shard_mode == "query_template" and mapping.shard_count > 1:
        query_template = mapping.incremental_query or mapping.source_query
        if not contains_shard_placeholders(str(query_template or "")):
            raise ValueError(
                f"Mapping {mapping.source_container}->{mapping.target_table} uses shard_mode=query_template "
                "but source_query/incremental_query does not contain shard placeholders."
            )
    return mapping


def load_config(path: str | Path) -> MigrationConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    source_raw = dict(_require(raw, "source"))
    source_key = source_raw.get("key")
    if not source_key:
        key_env = source_raw.get("key_env")
        if key_env:
            source_key = os.getenv(key_env)
    if not source_key:
        raise ValueError(
            "Cosmos source key is missing. Set source.key or source.key_env in config."
        )

    source = CosmosConfig(
        endpoint=_require(source_raw, "endpoint"),
        key=source_key,
        database=_require(source_raw, "database"),
    )

    target_raw = dict(_require(raw, "target"))
    target_project = target_raw.get("project") or os.getenv("GOOGLE_CLOUD_PROJECT", "")
    if not target_project:
        raise ValueError(
            "Spanner target project is missing. Set target.project or GOOGLE_CLOUD_PROJECT."
        )
    target = SpannerConfig(
        project=str(target_project),
        instance=_require(target_raw, "instance"),
        database=_require(target_raw, "database"),
    )

    runtime_raw = dict(raw.get("runtime", {}))
    raw_max_docs = runtime_raw.get("max_docs_per_container", {})
    parsed_max_docs: dict[str, int | None] = {}
    for container_name, limit in raw_max_docs.items():
        if limit is None:
            parsed_max_docs[str(container_name)] = None
            continue
        parsed_limit = int(limit)
        parsed_max_docs[str(container_name)] = parsed_limit if parsed_limit > 0 else None

    runtime = RuntimeConfig(
        deployment_environment=str(runtime_raw.get("deployment_environment", "dev")).lower(),
        batch_size=int(runtime_raw.get("batch_size", 500)),
        query_page_size=int(runtime_raw.get("query_page_size", 200)),
        dry_run=_parse_bool(runtime_raw.get("dry_run", False), default=False),
        log_level=str(runtime_raw.get("log_level", "INFO")),
        log_format=str(runtime_raw.get("log_format", "text")).lower(),
        watermark_state_file=str(
            runtime_raw.get("watermark_state_file", "state/watermarks.json")
        ),
        watermark_overlap_seconds=int(runtime_raw.get("watermark_overlap_seconds", 5)),
        flush_watermark_each_mapping=_parse_bool(
            runtime_raw.get("flush_watermark_each_mapping", True), default=True
        ),
        error_mode=str(runtime_raw.get("error_mode", "fail")).lower(),
        dlq_file_path=str(runtime_raw.get("dlq_file_path", "state/dead_letter.jsonl")),
        metrics_file_path=str(runtime_raw.get("metrics_file_path", "")),
        metrics_format=str(runtime_raw.get("metrics_format", "prometheus")).lower(),
        retry_attempts=int(runtime_raw.get("retry_attempts", 5)),
        retry_initial_delay_seconds=float(
            runtime_raw.get("retry_initial_delay_seconds", 0.5)
        ),
        retry_max_delay_seconds=float(runtime_raw.get("retry_max_delay_seconds", 15.0)),
        retry_backoff_multiplier=float(runtime_raw.get("retry_backoff_multiplier", 2.0)),
        retry_jitter_seconds=float(runtime_raw.get("retry_jitter_seconds", 0.25)),
        max_docs_per_container=parsed_max_docs,
        lease_file=str(runtime_raw.get("lease_file", "")),
        progress_file=str(runtime_raw.get("progress_file", "")),
        run_id=str(runtime_raw.get("run_id", "")),
        worker_id=str(runtime_raw.get("worker_id", os.getenv("MIGRATION_WORKER_ID", ""))),
        lease_duration_seconds=int(runtime_raw.get("lease_duration_seconds", 120)),
        heartbeat_interval_seconds=int(runtime_raw.get("heartbeat_interval_seconds", 30)),
        reader_cursor_state_file=str(runtime_raw.get("reader_cursor_state_file", "")),
        release_gate_file=str(runtime_raw.get("release_gate_file", "")),
        release_gate_scope=str(runtime_raw.get("release_gate_scope", "")),
        release_gate_max_age_hours=int(runtime_raw.get("release_gate_max_age_hours", 72)),
        require_stage_rehearsal_for_prod=_parse_bool(
            runtime_raw.get("require_stage_rehearsal_for_prod", False),
            default=False,
        ),
    )

    if runtime.deployment_environment not in {"dev", "stage", "prod"}:
        raise ValueError("runtime.deployment_environment must be one of: dev, stage, prod")
    if runtime.error_mode not in {"fail", "skip"}:
        raise ValueError("runtime.error_mode must be one of: fail, skip")
    if runtime.log_format not in {"text", "json"}:
        raise ValueError("runtime.log_format must be one of: text, json")
    if runtime.metrics_format not in {"prometheus", "json"}:
        raise ValueError("runtime.metrics_format must be one of: prometheus, json")
    if runtime.batch_size <= 0:
        raise ValueError("runtime.batch_size must be > 0")
    if runtime.query_page_size <= 0:
        raise ValueError("runtime.query_page_size must be > 0")
    if runtime.watermark_overlap_seconds < 0:
        raise ValueError("runtime.watermark_overlap_seconds must be >= 0")
    if runtime.retry_attempts < 1:
        raise ValueError("runtime.retry_attempts must be >= 1")
    if runtime.retry_initial_delay_seconds < 0:
        raise ValueError("runtime.retry_initial_delay_seconds must be >= 0")
    if runtime.retry_max_delay_seconds < 0:
        raise ValueError("runtime.retry_max_delay_seconds must be >= 0")
    if runtime.retry_backoff_multiplier < 1:
        raise ValueError("runtime.retry_backoff_multiplier must be >= 1")
    if runtime.retry_jitter_seconds < 0:
        raise ValueError("runtime.retry_jitter_seconds must be >= 0")
    if runtime.lease_duration_seconds <= 0:
        raise ValueError("runtime.lease_duration_seconds must be > 0")
    if runtime.heartbeat_interval_seconds <= 0:
        raise ValueError("runtime.heartbeat_interval_seconds must be > 0")
    if runtime.lease_file and runtime.heartbeat_interval_seconds >= runtime.lease_duration_seconds:
        raise ValueError(
            "runtime.heartbeat_interval_seconds must be less than "
            "runtime.lease_duration_seconds when runtime.lease_file is configured"
        )
    if runtime.progress_file and not runtime.run_id:
        raise ValueError(
            "runtime.run_id must be set when runtime.progress_file is configured"
        )
    if runtime.release_gate_file and not runtime.release_gate_scope:
        raise ValueError(
            "runtime.release_gate_scope must be set when runtime.release_gate_file is configured"
        )
    if runtime.release_gate_max_age_hours <= 0:
        raise ValueError("runtime.release_gate_max_age_hours must be > 0")
    if runtime.require_stage_rehearsal_for_prod and runtime.deployment_environment != "prod":
        raise ValueError(
            "runtime.require_stage_rehearsal_for_prod is only valid when runtime.deployment_environment=prod"
        )
    if runtime.require_stage_rehearsal_for_prod and not runtime.release_gate_file:
        raise ValueError(
            "runtime.release_gate_file must be configured when runtime.require_stage_rehearsal_for_prod is true"
        )
    if runtime.require_stage_rehearsal_for_prod and not runtime.release_gate_scope:
        raise ValueError(
            "runtime.release_gate_scope must be configured when runtime.require_stage_rehearsal_for_prod is true"
        )

    mappings_raw = raw.get("mappings", [])
    if not mappings_raw:
        raise ValueError("No mappings found. Add at least one mapping in `mappings`.")
    mappings = [_parse_mapping(m) for m in mappings_raw]

    return MigrationConfig(source=source, target=target, runtime=runtime, mappings=mappings)
