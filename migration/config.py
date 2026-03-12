from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


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
    batch_size: int = 500
    query_page_size: int = 200
    dry_run: bool = False
    log_level: str = "INFO"
    watermark_state_file: str = "state/watermarks.json"
    watermark_overlap_seconds: int = 5
    flush_watermark_each_mapping: bool = True
    error_mode: str = "fail"  # fail | skip
    dlq_file_path: str = "state/dead_letter.jsonl"
    retry_attempts: int = 5
    retry_initial_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 15.0
    retry_backoff_multiplier: float = 2.0
    retry_jitter_seconds: float = 0.25
    max_docs_per_container: dict[str, int | None] = field(default_factory=dict)


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
    )
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
    invalid_validation_columns = [
        col for col in mapping.validation_columns if col not in available_targets
    ]
    if invalid_validation_columns:
        raise ValueError(
            f"validation_columns {invalid_validation_columns} are not mapped output columns "
            f"in mapping {mapping.source_container}->{mapping.target_table}."
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
    target = SpannerConfig(
        project=target_raw.get("project") or os.getenv("GOOGLE_CLOUD_PROJECT", ""),
        instance=_require(target_raw, "instance"),
        database=_require(target_raw, "database"),
    )
    if not target.project:
        raise ValueError(
            "Spanner target project is missing. Set target.project or GOOGLE_CLOUD_PROJECT."
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
        batch_size=int(runtime_raw.get("batch_size", 500)),
        query_page_size=int(runtime_raw.get("query_page_size", 200)),
        dry_run=_parse_bool(runtime_raw.get("dry_run", False), default=False),
        log_level=str(runtime_raw.get("log_level", "INFO")),
        watermark_state_file=str(
            runtime_raw.get("watermark_state_file", "state/watermarks.json")
        ),
        watermark_overlap_seconds=int(runtime_raw.get("watermark_overlap_seconds", 5)),
        flush_watermark_each_mapping=_parse_bool(
            runtime_raw.get("flush_watermark_each_mapping", True), default=True
        ),
        error_mode=str(runtime_raw.get("error_mode", "fail")).lower(),
        dlq_file_path=str(runtime_raw.get("dlq_file_path", "state/dead_letter.jsonl")),
        retry_attempts=int(runtime_raw.get("retry_attempts", 5)),
        retry_initial_delay_seconds=float(
            runtime_raw.get("retry_initial_delay_seconds", 0.5)
        ),
        retry_max_delay_seconds=float(runtime_raw.get("retry_max_delay_seconds", 15.0)),
        retry_backoff_multiplier=float(runtime_raw.get("retry_backoff_multiplier", 2.0)),
        retry_jitter_seconds=float(runtime_raw.get("retry_jitter_seconds", 0.25)),
        max_docs_per_container=parsed_max_docs,
    )

    if runtime.error_mode not in {"fail", "skip"}:
        raise ValueError("runtime.error_mode must be one of: fail, skip")
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

    mappings_raw = raw.get("mappings", [])
    if not mappings_raw:
        raise ValueError("No mappings found. Add at least one mapping in `mappings`.")
    mappings = [_parse_mapping(m) for m in mappings_raw]

    return MigrationConfig(source=source, target=target, runtime=runtime, mappings=mappings)
