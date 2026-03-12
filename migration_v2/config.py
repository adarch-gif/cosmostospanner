from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class FirestoreTargetConfig:
    project: str
    database: str = "(default)"
    collection: str = "cosmos_router_v2"


@dataclass
class SpannerTargetConfig:
    project: str
    instance: str
    database: str
    table: str


@dataclass
class RoutingConfig:
    firestore_lt_bytes: int = 1_048_576
    spanner_max_payload_bytes: int = 10_485_760
    payload_size_overhead_bytes: int = 2_048


@dataclass
class RuntimeV2Config:
    mode: str = "full"  # full | incremental
    batch_size: int = 200
    dry_run: bool = False
    log_level: str = "INFO"
    error_mode: str = "fail"  # fail | skip
    state_file: str = "state/v2_watermarks.json"
    route_registry_file: str = "state/v2_route_registry.json"
    dlq_file_path: str = "state/v2_dead_letter.jsonl"
    flush_state_each_batch: bool = True
    retry_attempts: int = 5
    retry_initial_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 15.0
    retry_backoff_multiplier: float = 2.0
    retry_jitter_seconds: float = 0.25
    max_records_per_job: dict[str, int | None] = field(default_factory=dict)


@dataclass
class BaseJobConfig:
    name: str
    api: str  # mongodb | cassandra
    route_namespace: str
    key_fields: list[str]
    source_query: str | None = None
    incremental_field: str | None = None
    enabled: bool = True
    page_size: int = 500


@dataclass
class MongoJobConfig(BaseJobConfig):
    connection_string: str = ""
    database: str = ""
    collection: str = ""


@dataclass
class CassandraJobConfig(BaseJobConfig):
    contact_points: list[str] = field(default_factory=list)
    port: int = 10350
    username: str = ""
    password: str = ""
    keyspace: str = ""
    table: str = ""


@dataclass
class PipelineV2Config:
    runtime: RuntimeV2Config
    routing: RoutingConfig
    firestore_target: FirestoreTargetConfig
    spanner_target: SpannerTargetConfig
    jobs: list[MongoJobConfig | CassandraJobConfig]


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


def _string_from_env_or_value(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if value:
        return str(value)
    env_key = raw.get(f"{key}_env")
    if env_key:
        env_value = os.getenv(str(env_key), "")
        if env_value:
            return env_value
    return ""


def _parse_key_fields(raw: Any, job_name: str) -> list[str]:
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"Job {job_name} key_fields must be a non-empty list.")
    parsed: list[str] = []
    for value in raw:
        if not isinstance(value, str) or not value:
            raise ValueError(f"Job {job_name} key_fields must contain non-empty strings.")
        parsed.append(value)
    return parsed


def _parse_runtime(raw: dict[str, Any]) -> RuntimeV2Config:
    runtime_raw = dict(raw.get("runtime", {}))
    max_records_raw = runtime_raw.get("max_records_per_job", {})
    max_records_per_job: dict[str, int | None] = {}
    for job_name, limit in max_records_raw.items():
        if limit is None:
            max_records_per_job[str(job_name)] = None
            continue
        parsed = int(limit)
        max_records_per_job[str(job_name)] = parsed if parsed > 0 else None

    runtime = RuntimeV2Config(
        mode=str(runtime_raw.get("mode", "full")).lower(),
        batch_size=int(runtime_raw.get("batch_size", 200)),
        dry_run=_parse_bool(runtime_raw.get("dry_run", False), default=False),
        log_level=str(runtime_raw.get("log_level", "INFO")),
        error_mode=str(runtime_raw.get("error_mode", "fail")).lower(),
        state_file=str(runtime_raw.get("state_file", "state/v2_watermarks.json")),
        route_registry_file=str(
            runtime_raw.get("route_registry_file", "state/v2_route_registry.json")
        ),
        dlq_file_path=str(runtime_raw.get("dlq_file_path", "state/v2_dead_letter.jsonl")),
        flush_state_each_batch=_parse_bool(
            runtime_raw.get("flush_state_each_batch", True),
            default=True,
        ),
        retry_attempts=int(runtime_raw.get("retry_attempts", 5)),
        retry_initial_delay_seconds=float(
            runtime_raw.get("retry_initial_delay_seconds", 0.5)
        ),
        retry_max_delay_seconds=float(runtime_raw.get("retry_max_delay_seconds", 15.0)),
        retry_backoff_multiplier=float(runtime_raw.get("retry_backoff_multiplier", 2.0)),
        retry_jitter_seconds=float(runtime_raw.get("retry_jitter_seconds", 0.25)),
        max_records_per_job=max_records_per_job,
    )

    if runtime.mode not in {"full", "incremental"}:
        raise ValueError("runtime.mode must be one of: full, incremental")
    if runtime.error_mode not in {"fail", "skip"}:
        raise ValueError("runtime.error_mode must be one of: fail, skip")
    if runtime.batch_size <= 0:
        raise ValueError("runtime.batch_size must be > 0")
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
    return runtime


def _parse_routing(raw: dict[str, Any]) -> RoutingConfig:
    routing_raw = dict(raw.get("routing", {}))
    routing = RoutingConfig(
        firestore_lt_bytes=int(routing_raw.get("firestore_lt_bytes", 1_048_576)),
        spanner_max_payload_bytes=int(
            routing_raw.get("spanner_max_payload_bytes", 10_485_760)
        ),
        payload_size_overhead_bytes=int(
            routing_raw.get("payload_size_overhead_bytes", 2_048)
        ),
    )
    if routing.firestore_lt_bytes <= 0:
        raise ValueError("routing.firestore_lt_bytes must be > 0")
    if routing.spanner_max_payload_bytes < routing.firestore_lt_bytes:
        raise ValueError(
            "routing.spanner_max_payload_bytes must be >= routing.firestore_lt_bytes"
        )
    if routing.payload_size_overhead_bytes < 0:
        raise ValueError("routing.payload_size_overhead_bytes must be >= 0")
    return routing


def _parse_firestore_target(raw: dict[str, Any]) -> FirestoreTargetConfig:
    firestore_raw = dict(_require(dict(_require(raw, "targets")), "firestore"))
    project = firestore_raw.get("project") or os.getenv("GOOGLE_CLOUD_PROJECT", "")
    if not project:
        raise ValueError(
            "targets.firestore.project is missing. Set it or GOOGLE_CLOUD_PROJECT."
        )
    return FirestoreTargetConfig(
        project=str(project),
        database=str(firestore_raw.get("database", "(default)")),
        collection=str(firestore_raw.get("collection", "cosmos_router_v2")),
    )


def _parse_spanner_target(raw: dict[str, Any]) -> SpannerTargetConfig:
    spanner_raw = dict(_require(dict(_require(raw, "targets")), "spanner"))
    project = spanner_raw.get("project") or os.getenv("GOOGLE_CLOUD_PROJECT", "")
    if not project:
        raise ValueError("targets.spanner.project is missing. Set it or GOOGLE_CLOUD_PROJECT.")
    return SpannerTargetConfig(
        project=str(project),
        instance=str(_require(spanner_raw, "instance")),
        database=str(_require(spanner_raw, "database")),
        table=str(_require(spanner_raw, "table")),
    )


def _parse_jobs(raw: dict[str, Any]) -> list[MongoJobConfig | CassandraJobConfig]:
    jobs_raw = raw.get("jobs", [])
    if not jobs_raw:
        raise ValueError("No jobs found. Add at least one entry in jobs.")

    parsed_jobs: list[MongoJobConfig | CassandraJobConfig] = []
    seen_names: set[str] = set()
    for entry in jobs_raw:
        job_name = str(_require(entry, "name"))
        if job_name in seen_names:
            raise ValueError(f"Duplicate job name: {job_name}")
        seen_names.add(job_name)

        api = str(_require(entry, "api")).lower()
        common = {
            "name": job_name,
            "api": api,
            "route_namespace": str(
                entry.get("route_namespace")
                or f"{api}.{entry.get('database') or entry.get('keyspace')}.{entry.get('collection') or entry.get('table')}"
            ),
            "key_fields": _parse_key_fields(entry.get("key_fields"), job_name),
            "source_query": entry.get("source_query"),
            "incremental_field": entry.get("incremental_field"),
            "enabled": _parse_bool(entry.get("enabled", True), default=True),
            "page_size": int(entry.get("page_size", 500)),
        }
        if common["page_size"] <= 0:
            raise ValueError(f"Job {job_name} page_size must be > 0")

        if api == "mongodb":
            connection_string = _string_from_env_or_value(entry, "connection_string")
            if not connection_string:
                raise ValueError(
                    f"Job {job_name} requires connection_string or connection_string_env."
                )
            parsed_jobs.append(
                MongoJobConfig(
                    **common,
                    connection_string=connection_string,
                    database=str(_require(entry, "database")),
                    collection=str(_require(entry, "collection")),
                )
            )
            continue

        if api == "cassandra":
            username = _string_from_env_or_value(entry, "username")
            password = _string_from_env_or_value(entry, "password")
            if not username or not password:
                raise ValueError(
                    f"Job {job_name} requires username/password or *_env variants."
                )
            contact_points_raw = entry.get("contact_points", [])
            if not isinstance(contact_points_raw, list) or not contact_points_raw:
                raise ValueError(
                    f"Job {job_name} contact_points must be a non-empty list."
                )
            contact_points = [str(point) for point in contact_points_raw]
            parsed_jobs.append(
                CassandraJobConfig(
                    **common,
                    contact_points=contact_points,
                    port=int(entry.get("port", 10350)),
                    username=username,
                    password=password,
                    keyspace=str(_require(entry, "keyspace")),
                    table=str(_require(entry, "table")),
                )
            )
            continue

        raise ValueError(f"Unsupported api for job {job_name}: {api}")
    return parsed_jobs


def load_v2_config(path: str | Path) -> PipelineV2Config:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    runtime = _parse_runtime(raw)
    routing = _parse_routing(raw)
    firestore_target = _parse_firestore_target(raw)
    spanner_target = _parse_spanner_target(raw)
    jobs = _parse_jobs(raw)

    return PipelineV2Config(
        runtime=runtime,
        routing=routing,
        firestore_target=firestore_target,
        spanner_target=spanner_target,
        jobs=jobs,
    )

