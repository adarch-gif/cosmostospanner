from __future__ import annotations

import json
import logging
from typing import Any, Iterable

from migration.resume import StreamResumeState, build_resume_scope
from migration.retry_utils import RetryPolicy, iter_with_retry
from migration_v2.config import MongoJobConfig
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

LOGGER = logging.getLogger("v2.mongo_source")


class MongoCosmosSourceAdapter:
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
        job: MongoJobConfig,
        *,
        mode: str,
        watermark: object | None,
        max_records: int | None,
        shard_index: int | None = None,
        shard_count: int = 1,
        resume_state: StreamResumeState | None = None,
    ) -> Iterable[CanonicalRecord]:
        from pymongo import MongoClient  # Imported lazily for optional dependency ergonomics.
        state = resume_state or StreamResumeState()
        seen = 0
        base_filter = self._build_query_filter(
            job,
            mode=mode,
            watermark=watermark,
            shard_index=shard_index,
            shard_count=shard_count,
        )
        sort_spec = self._sort_spec(job, mode)
        state.ensure_scope(
            build_resume_scope(
                job_name=job.name,
                mode=mode,
                base_filter=base_filter,
                sort_spec=sort_spec,
                shard_index=shard_index,
                shard_count=shard_count,
                watermark=watermark,
            )
        )

        def open_stream() -> Iterable[CanonicalRecord]:
            nonlocal seen
            query_filter = dict(base_filter)
            if mode == "incremental" and job.incremental_field:
                resume_watermark = state.resume_watermark(watermark)
                if resume_watermark is not None:
                    query_filter[job.incremental_field] = {"$gte": resume_watermark}

            LOGGER.info(
                "Mongo source job %s query=%s mode=%s watermark=%s resume_key=%s",
                job.name,
                query_filter,
                mode,
                query_filter.get(job.incremental_field) if job.incremental_field else watermark,
                state.last_source_key or None,
            )

            client = MongoClient(job.connection_string)
            try:
                collection = client[job.database][job.collection]
                cursor = collection.find(
                    query_filter,
                    no_cursor_timeout=True,
                    batch_size=job.page_size,
                ).sort(sort_spec)
                try:
                    for raw_document in cursor:
                        payload = to_jsonable(raw_document)
                        if not isinstance(payload, dict):
                            raise ValueError(f"Mongo document in job {job.name} is not an object.")
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
                            source_api="mongodb",
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
                    cursor.close()
            finally:
                client.close()

        def on_retry(error: BaseException, attempt: int) -> None:
            LOGGER.warning(
                "Resuming Mongo source job %s after stream failure at source_key=%s watermark=%s "
                "(attempt %s): %s",
                job.name,
                state.last_source_key or None,
                state.last_watermark,
                attempt,
                error,
            )

        yield from iter_with_retry(
            open_stream,
            operation_name=f"mongo_iter:{job.name}",
            policy=self._retry_policy,
            logger=LOGGER,
            on_retry=on_retry,
        )

    def _build_query_filter(
        self,
        job: MongoJobConfig,
        *,
        mode: str,
        watermark: object | None,
        shard_index: int | None,
        shard_count: int,
    ) -> dict[str, Any]:
        query_filter: dict[str, Any] = {}
        if job.source_query:
            try:
                source_query = job.source_query
                if job.shard_mode == "query_template" and shard_index is not None and shard_count > 1:
                    source_query = apply_shard_placeholders(
                        source_query,
                        shard_index=shard_index,
                        shard_count=shard_count,
                    )
                query_filter = dict(json.loads(source_query))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Job {job.name} source_query must be valid JSON object.") from exc

        if mode == "incremental" and job.incremental_field and watermark is not None:
            query_filter = dict(query_filter)
            query_filter[job.incremental_field] = {"$gte": watermark}
        return query_filter

    def _sort_spec(self, job: MongoJobConfig, mode: str) -> list[tuple[str, int]]:
        sort_fields: list[str] = []
        if mode == "incremental" and job.incremental_field:
            sort_fields.append(job.incremental_field)
        sort_fields.extend(field for field in job.key_fields if field not in sort_fields)
        if "_id" not in sort_fields:
            sort_fields.append("_id")
        return [(field, 1) for field in sort_fields]
