from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from migration.config import MigrationConfig, TableMapping
from migration.control_plane_backend import (
    SpannerControlPlaneBackend,
    is_spanner_control_plane_path,
)
from migration.json_state_backend import JsonStateBackend
from migration_v2.config import CassandraJobConfig, MongoJobConfig, PipelineV2Config

RELEASE_GATE_STATUS_PASSED = "passed"
RELEASE_GATE_STATUS_FAILED = "failed"
VALID_RELEASE_GATE_STATUSES = {RELEASE_GATE_STATUS_PASSED, RELEASE_GATE_STATUS_FAILED}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _require_local_backend(backend: JsonStateBackend | None) -> JsonStateBackend:
    if backend is None:
        raise RuntimeError("Local release-gate backend is not configured.")
    return backend


class ReleaseGateRuntime(Protocol):
    deployment_environment: str
    release_gate_file: str
    release_gate_scope: str
    release_gate_max_age_hours: int
    require_stage_rehearsal_for_prod: bool


@dataclass
class ReleaseGateRecord:
    pipeline: str
    environment: str
    scope: str
    status: str
    attested_at: str
    logical_fingerprint: str
    checks: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


def make_release_gate_key(pipeline: str, environment: str, scope: str) -> str:
    return f"{pipeline}:{environment}:{scope}"


class ReleaseGateStore:
    def __init__(self, location: str) -> None:
        self.backend = (
            None if is_spanner_control_plane_path(location) else JsonStateBackend(location)
        )
        self.spanner = (
            SpannerControlPlaneBackend(location)
            if is_spanner_control_plane_path(location)
            else None
        )
        self.data: dict[str, dict[str, Any]] = {}
        self._dirty_keys: set[str] = set()
        self._load()

    def _serialize_record(self, record: ReleaseGateRecord) -> dict[str, Any]:
        return {
            "pipeline": record.pipeline,
            "environment": record.environment,
            "scope": record.scope,
            "status": record.status,
            "attested_at": record.attested_at,
            "logical_fingerprint": record.logical_fingerprint,
            "checks": sorted({str(item) for item in record.checks}),
            "details": dict(record.details),
        }

    def _deserialize_record(self, raw: Any) -> ReleaseGateRecord | None:
        if not raw:
            return None
        if not isinstance(raw, dict):
            raise ValueError("Release-gate records must be JSON objects.")
        status = str(raw.get("status", "")).lower()
        if status not in VALID_RELEASE_GATE_STATUSES:
            return None
        checks_raw = raw.get("checks", [])
        checks = []
        if isinstance(checks_raw, list):
            checks = sorted({str(item) for item in checks_raw})
        details_raw = raw.get("details", {})
        details = dict(details_raw) if isinstance(details_raw, dict) else {}
        return ReleaseGateRecord(
            pipeline=str(raw.get("pipeline", "")),
            environment=str(raw.get("environment", "")),
            scope=str(raw.get("scope", "")),
            status=status,
            attested_at=str(raw.get("attested_at", "")),
            logical_fingerprint=str(raw.get("logical_fingerprint", "")),
            checks=checks,
            details=details,
        )

    def _normalize_data(self, raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
        normalized: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            record = self._deserialize_record(value)
            if record is not None:
                normalized[str(key)] = self._serialize_record(record)
        return normalized

    def _load(self) -> None:
        if self.spanner is not None:
            self.data = {}
        else:
            backend = _require_local_backend(self.backend)
            self.data = self._normalize_data(backend.read_json_object())
        self._dirty_keys = set()

    def _load_spanner_key(self, key: str) -> None:
        if self.spanner is None or key in self.data:
            return
        row = self.spanner.get(key)
        if row is not None and isinstance(row.payload, dict):
            record = self._normalize_data({key: row.payload}).get(key)
            if record is not None:
                self.data[key] = record

    def get(self, key: str) -> ReleaseGateRecord | None:
        self._load_spanner_key(key)
        return self._deserialize_record(self.data.get(key))

    def set(self, key: str, record: ReleaseGateRecord) -> None:
        if record.status not in VALID_RELEASE_GATE_STATUSES:
            raise ValueError(f"Invalid release-gate status: {record.status}")
        self.data[key] = self._serialize_record(record)
        self._dirty_keys.add(key)

    def flush(self) -> None:
        dirty_keys = sorted(self._dirty_keys)
        if not dirty_keys:
            return

        if self.spanner is not None:
            spanner_backend = self.spanner
            persisted: dict[str, dict[str, Any]] = {}

            def txn_fn(transaction: Any) -> None:
                current_rows = spanner_backend._read_records_with_reader(transaction, dirty_keys)
                current_data = self._normalize_data(
                    {
                        key: row.payload
                        for key, row in current_rows.items()
                        if isinstance(row.payload, dict)
                    }
                )
                for key in dirty_keys:
                    current_record = self._deserialize_record(current_data.get(key))
                    candidate_record = self._deserialize_record(self.data.get(key))
                    if candidate_record is None:
                        continue
                    if (
                        current_record is not None
                        and current_record.attested_at > candidate_record.attested_at
                    ):
                        persisted[key] = self._serialize_record(current_record)
                        continue
                    serialized = self._serialize_record(candidate_record)
                    spanner_backend.upsert(
                        transaction,
                        record_key=key,
                        payload=serialized,
                    )
                    persisted[key] = serialized

            spanner_backend.run_in_transaction(txn_fn)
            self.data.update(persisted)
            self._dirty_keys = set()
            return

        backend = _require_local_backend(self.backend)

        def merge_fn(current_data: dict[str, Any]) -> dict[str, Any]:
            normalized_current = self._normalize_data(current_data)
            merged = dict(normalized_current)
            for key in dirty_keys:
                current_record = self._deserialize_record(normalized_current.get(key))
                candidate_record = self._deserialize_record(self.data.get(key))
                if candidate_record is None:
                    continue
                if (
                    current_record is not None
                    and current_record.attested_at > candidate_record.attested_at
                ):
                    merged[key] = self._serialize_record(current_record)
                else:
                    merged[key] = self._serialize_record(candidate_record)
            return merged

        self.data = self._normalize_data(backend.merge_write_json_object(merge_fn, ensure_ascii=False))
        self._dirty_keys = set()


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _fingerprint_mapping(mapping: TableMapping) -> dict[str, Any]:
    return {
        "source_container": mapping.source_container,
        "target_table": mapping.target_table,
        "key_columns": list(mapping.key_columns),
        "mode": mapping.mode,
        "source_query": mapping.source_query,
        "incremental_query": mapping.incremental_query,
        "validation_columns": list(mapping.validation_columns),
        "static_columns": dict(mapping.static_columns),
        "shard_count": mapping.shard_count,
        "shard_mode": mapping.shard_mode,
        "shard_key_source": mapping.shard_key_source,
        "columns": [
            {
                "target": column.target,
                "source": column.source,
                "converter": column.converter,
                "default": column.default,
                "required": column.required,
            }
            for column in mapping.columns
        ],
        "delete_rule": (
            {
                "field": mapping.delete_rule.field,
                "equals": mapping.delete_rule.equals,
            }
            if mapping.delete_rule is not None
            else None
        ),
    }


def logical_fingerprint_v1(
    config: MigrationConfig,
    mappings: list[TableMapping] | None = None,
) -> str:
    selected = mappings if mappings is not None else config.mappings
    payload = {
        "pipeline": "v1",
        "mappings": [_fingerprint_mapping(mapping) for mapping in selected],
        "runtime": {
            "watermark_overlap_seconds": config.runtime.watermark_overlap_seconds,
            "batch_size": config.runtime.batch_size,
            "query_page_size": config.runtime.query_page_size,
        },
    }
    return _stable_hash(payload)


def _fingerprint_job(job: MongoJobConfig | CassandraJobConfig) -> dict[str, Any]:
    base = {
        "name": job.name,
        "api": job.api,
        "route_namespace": job.route_namespace,
        "key_fields": list(job.key_fields),
        "source_query": job.source_query,
        "incremental_field": job.incremental_field,
        "page_size": job.page_size,
        "shard_count": job.shard_count,
        "shard_mode": job.shard_mode,
    }
    if isinstance(job, MongoJobConfig):
        base.update(
            {
                "database": job.database,
                "collection": job.collection,
            }
        )
    else:
        base.update(
            {
                "keyspace": job.keyspace,
                "table": job.table,
            }
        )
    return base


def logical_fingerprint_v2(
    config: PipelineV2Config,
    jobs: list[MongoJobConfig | CassandraJobConfig] | None = None,
) -> str:
    selected = jobs if jobs is not None else config.jobs
    payload = {
        "pipeline": "v2",
        "routing": asdict(config.routing),
        "jobs": [_fingerprint_job(job) for job in selected],
    }
    return _stable_hash(payload)


def verify_stage_release_gate(
    *,
    store: ReleaseGateStore,
    pipeline: str,
    scope: str,
    logical_fingerprint: str,
    max_age_hours: int,
    required_checks: list[str] | None = None,
    now: datetime | None = None,
) -> str | None:
    record = store.get(make_release_gate_key(pipeline, "stage", scope))
    if record is None:
        return f"Missing successful stage release-gate record for {pipeline} scope={scope!r}."
    if record.status != RELEASE_GATE_STATUS_PASSED:
        return f"Stage release-gate record for {pipeline} scope={scope!r} is not marked passed."
    if record.logical_fingerprint != logical_fingerprint:
        return (
            f"Stage release-gate logical fingerprint mismatch for {pipeline} scope={scope!r}."
        )
    try:
        attested_at = datetime.fromisoformat(record.attested_at)
    except ValueError:
        return f"Stage release-gate record for {pipeline} scope={scope!r} has invalid attested_at."
    if attested_at.tzinfo is None:
        attested_at = attested_at.replace(tzinfo=UTC)
    expiry = attested_at + timedelta(hours=max_age_hours)
    if expiry < (now or _utc_now()):
        return f"Stage release-gate record for {pipeline} scope={scope!r} is older than {max_age_hours} hour(s)."
    required = set(required_checks or ["preflight", "validation"])
    if not required.issubset(set(record.checks)):
        missing = sorted(required.difference(record.checks))
        return (
            f"Stage release-gate record for {pipeline} scope={scope!r} is missing checks: {missing}."
        )
    return None


def enforce_stage_rehearsal_or_raise(
    *,
    runtime: ReleaseGateRuntime,
    pipeline: str,
    logical_fingerprint: str,
    required_checks: list[str] | None = None,
) -> None:
    if runtime.deployment_environment != "prod" or not runtime.require_stage_rehearsal_for_prod:
        return
    if not runtime.release_gate_file:
        raise RuntimeError(
            "runtime.release_gate_file must be configured when stage rehearsal is required for prod."
        )
    if not runtime.release_gate_scope:
        raise RuntimeError(
            "runtime.release_gate_scope must be configured when stage rehearsal is required for prod."
        )
    store = ReleaseGateStore(runtime.release_gate_file)
    error = verify_stage_release_gate(
        store=store,
        pipeline=pipeline,
        scope=runtime.release_gate_scope,
        logical_fingerprint=logical_fingerprint,
        max_age_hours=runtime.release_gate_max_age_hours,
        required_checks=required_checks,
    )
    if error is not None:
        raise RuntimeError(error)


def build_release_gate_record(
    *,
    pipeline: str,
    environment: str,
    scope: str,
    status: str,
    logical_fingerprint: str,
    checks: list[str],
    details: dict[str, Any] | None = None,
    attested_at: str | None = None,
) -> ReleaseGateRecord:
    return ReleaseGateRecord(
        pipeline=pipeline,
        environment=environment,
        scope=scope,
        status=status,
        attested_at=attested_at or _utc_now_iso(),
        logical_fingerprint=logical_fingerprint,
        checks=sorted({str(item) for item in checks}),
        details=dict(details or {}),
    )
