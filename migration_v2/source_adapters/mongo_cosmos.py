from __future__ import annotations

import json
import logging
from typing import Any, Iterable

from migration_v2.config import MongoJobConfig
from migration_v2.models import CanonicalRecord, utc_now_iso
from migration_v2.utils import (
    build_source_key,
    json_size_bytes,
    nested_get,
    payload_checksum,
    to_jsonable,
)

LOGGER = logging.getLogger("v2.mongo_source")


class MongoCosmosSourceAdapter:
    def iter_records(
        self,
        job: MongoJobConfig,
        *,
        mode: str,
        watermark: object | None,
        max_records: int | None,
    ) -> Iterable[CanonicalRecord]:
        query_filter: dict[str, Any] = {}
        if job.source_query:
            try:
                query_filter = dict(json.loads(job.source_query))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Job {job.name} source_query must be valid JSON object.") from exc

        if mode == "incremental" and job.incremental_field:
            if watermark is not None:
                query_filter = dict(query_filter)
                query_filter[job.incremental_field] = {"$gte": watermark}

        LOGGER.info(
            "Mongo source job %s query=%s mode=%s watermark=%s",
            job.name,
            query_filter,
            mode,
            watermark,
        )

        from pymongo import MongoClient  # Imported lazily for optional dependency ergonomics.

        client = MongoClient(job.connection_string)
        try:
            collection = client[job.database][job.collection]
            cursor = collection.find(query_filter, no_cursor_timeout=True, batch_size=job.page_size)
            if mode == "incremental" and job.incremental_field:
                cursor = cursor.sort(job.incremental_field, 1)

            seen = 0
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
                yield record
                seen += 1
                if max_records and seen >= max_records:
                    break
        finally:
            client.close()
