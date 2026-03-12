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
    error_mode: str = "fail"  # fail | skip
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
                    required=bool(entry.get("required", False)),
                )
            )
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
                        required=bool(value.get("required", False)),
                    )
                )
            else:
                raise ValueError(f"Unsupported column mapping for {target}: {value!r}")
        return rules

    raise ValueError("`columns` must be a list or dictionary.")


def _parse_mapping(raw: dict[str, Any]) -> TableMapping:
    delete_rule_raw = raw.get("delete_rule")
    delete_rule = None
    if delete_rule_raw:
        delete_rule = DeleteRule(
            field=_require(delete_rule_raw, "field"),
            equals=delete_rule_raw.get("equals", True),
        )

    mapping = TableMapping(
        source_container=_require(raw, "source_container"),
        target_table=_require(raw, "target_table"),
        key_columns=list(_require(raw, "key_columns")),
        columns=_parse_column_rules(_require(raw, "columns")),
        mode=str(raw.get("mode", "upsert")).lower(),
        source_query=raw.get("source_query", "SELECT * FROM c"),
        incremental_query=raw.get("incremental_query"),
        static_columns=dict(raw.get("static_columns", {})),
        delete_rule=delete_rule,
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
        dry_run=bool(runtime_raw.get("dry_run", False)),
        log_level=str(runtime_raw.get("log_level", "INFO")),
        watermark_state_file=str(
            runtime_raw.get("watermark_state_file", "state/watermarks.json")
        ),
        watermark_overlap_seconds=int(runtime_raw.get("watermark_overlap_seconds", 5)),
        error_mode=str(runtime_raw.get("error_mode", "fail")).lower(),
        max_docs_per_container=parsed_max_docs,
    )

    if runtime.error_mode not in {"fail", "skip"}:
        raise ValueError("runtime.error_mode must be one of: fail, skip")

    mappings_raw = raw.get("mappings", [])
    if not mappings_raw:
        raise ValueError("No mappings found. Add at least one mapping in `mappings`.")
    mappings = [_parse_mapping(m) for m in mappings_raw]

    return MigrationConfig(source=source, target=target, runtime=runtime, mappings=mappings)
