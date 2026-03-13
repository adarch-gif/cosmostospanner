from __future__ import annotations

import logging
from typing import Iterable

from migration.resume import StreamResumeState, build_resume_scope
from migration.retry_utils import RetryPolicy, iter_with_retry
from migration_v2.config import CassandraJobConfig
from migration_v2.models import CanonicalRecord, utc_now_iso
from migration_v2.state_store import compare_watermark_values
from migration_v2.utils import (
    build_source_key,
    json_size_bytes,
    nested_get,
    payload_checksum,
    to_jsonable,
)
from migration.sharding import apply_shard_placeholders

LOGGER = logging.getLogger("v2.cassandra_source")


class CassandraCosmosSourceAdapter:
    def __init__(self, retry_policy: RetryPolicy | None = None) -> None:
        self._retry_policy = retry_policy or RetryPolicy(
            attempts=1,
            initial_delay_seconds=0.0,
            max_delay_seconds=0.0,
            backoff_multiplier=1.0,
            jitter_seconds=0.0,
        )

    def iter_records(
        self,
        job: CassandraJobConfig,
        *,
        mode: str,
        watermark: object | None,
        max_records: int | None,
        shard_index: int | None = None,
        shard_count: int = 1,
        resume_state: StreamResumeState | None = None,
    ) -> Iterable[CanonicalRecord]:
        from cassandra.auth import PlainTextAuthProvider  # Lazy import for optional dependency ergonomics.
        from cassandra.cluster import Cluster

        state = resume_state or StreamResumeState()
        seen = 0
        state.ensure_scope(
            build_resume_scope(
                job_name=job.name,
                mode=mode,
                source_query=job.source_query or f"SELECT * FROM {job.table}",  # nosec B608
                shard_index=shard_index,
                shard_count=shard_count,
                watermark=watermark,
            )
        )

        def open_stream() -> Iterable[CanonicalRecord]:
            nonlocal seen
            auth_provider = PlainTextAuthProvider(username=job.username, password=job.password)
            cluster = Cluster(
                contact_points=job.contact_points,
                port=job.port,
                auth_provider=auth_provider,
            )
            session = cluster.connect()
            try:
                session.set_keyspace(job.keyspace)
                resume_watermark = state.resume_watermark(watermark)
                query, params = self._build_query(
                    job,
                    mode=mode,
                    watermark=resume_watermark,
                    shard_index=shard_index,
                    shard_count=shard_count,
                )
                LOGGER.info(
                    "Cassandra source job %s query=%s mode=%s watermark=%s resume_key=%s",
                    job.name,
                    query,
                    mode,
                    resume_watermark,
                    state.last_source_key or None,
                )

                statement = session.prepare(query) if params is not None else query
                rows = session.execute(statement, params) if params is not None else session.execute(statement)
                for row in rows:
                    row_dict = row._asdict()
                    payload = to_jsonable(row_dict)
                    if not isinstance(payload, dict):
                        raise ValueError(f"Cassandra row in job {job.name} is not an object.")
                    source_key = build_source_key(payload, job.key_fields)
                    watermark_value = None
                    event_ts = utc_now_iso()
                    if job.incremental_field:
                        try:
                            watermark_value = to_jsonable(nested_get(payload, job.incremental_field))
                            event_ts = str(watermark_value)
                        except KeyError:
                            watermark_value = None
                    if state.should_skip(
                        source_key=source_key,
                        watermark=watermark_value,
                        compare_watermarks=compare_watermark_values,
                    ):
                        continue

                    record = CanonicalRecord(
                        source_job=job.name,
                        source_api="cassandra",
                        source_namespace=job.route_namespace,
                        source_key=source_key,
                        route_key=f"{job.route_namespace}|{source_key}",
                        payload=payload,
                        payload_size_bytes=json_size_bytes(payload),
                        checksum=payload_checksum(payload),
                        event_ts=event_ts,
                        watermark_value=watermark_value,
                    )
                    state.mark(
                        source_key=record.source_key,
                        watermark=record.watermark_value,
                    )
                    seen += 1
                    yield record
                    if max_records and seen >= max_records:
                        return
            finally:
                session.shutdown()
                cluster.shutdown()

        def on_retry(error: BaseException, attempt: int) -> None:
            LOGGER.warning(
                "Resuming Cassandra source job %s after stream failure at source_key=%s watermark=%s "
                "(attempt %s). Resume relies on a stable source query order: %s",
                job.name,
                state.last_source_key or None,
                state.last_watermark,
                attempt,
                error,
            )

        yield from iter_with_retry(
            open_stream,
            operation_name=f"cassandra_iter:{job.name}",
            policy=self._retry_policy,
            logger=LOGGER,
            on_retry=on_retry,
        )

    def _build_query(
        self,
        job: CassandraJobConfig,
        *,
        mode: str,
        watermark: object | None,
        shard_index: int | None,
        shard_count: int,
    ) -> tuple[str, tuple[object, ...] | None]:
        query = job.source_query or f"SELECT * FROM {job.table}"  # nosec B608
        if job.shard_mode == "query_template" and shard_index is not None and shard_count > 1:
            query = apply_shard_placeholders(
                query,
                shard_index=shard_index,
                shard_count=shard_count,
            )
        params = None
        if mode == "incremental" and job.incremental_field and watermark is not None:
            if "%s" in query:
                params = (watermark,)
            elif not job.source_query:
                query = (
                    f"SELECT * FROM {job.table} WHERE {job.incremental_field} >= %s "  # nosec B608
                    "ALLOW FILTERING"
                )
                params = (watermark,)
            else:
                raise ValueError(
                    f"Job {job.name} incremental source_query must include a %s placeholder "
                    "for parameterized watermark binding."
                )
        return query, params
