from __future__ import annotations

import logging
from typing import Iterable

from migration_v2.config import CassandraJobConfig
from migration_v2.models import CanonicalRecord, utc_now_iso
from migration_v2.utils import (
    build_source_key,
    json_size_bytes,
    nested_get,
    payload_checksum,
    to_jsonable,
)

LOGGER = logging.getLogger("v2.cassandra_source")


class CassandraCosmosSourceAdapter:
    def iter_records(
        self,
        job: CassandraJobConfig,
        *,
        mode: str,
        watermark: object | None,
        max_records: int | None,
    ) -> Iterable[CanonicalRecord]:
        from cassandra.auth import PlainTextAuthProvider  # Lazy import for optional dependency ergonomics.
        from cassandra.cluster import Cluster

        auth_provider = PlainTextAuthProvider(username=job.username, password=job.password)
        cluster = Cluster(
            contact_points=job.contact_points,
            port=job.port,
            auth_provider=auth_provider,
        )
        session = cluster.connect()
        try:
            session.set_keyspace(job.keyspace)

            query = job.source_query or f"SELECT * FROM {job.table}"  # nosec B608
            params = None
            if mode == "incremental" and job.incremental_field:
                if watermark is not None:
                    if "%s" in query:
                        params = (watermark,)
                    elif not job.source_query:
                        query = (
                            f"SELECT * FROM {job.table} WHERE {job.incremental_field} > %s "  # nosec B608
                            "ALLOW FILTERING"
                        )
                        params = (watermark,)
                    else:
                        raise ValueError(
                            f"Job {job.name} incremental source_query must include a %s placeholder "
                            "for parameterized watermark binding."
                        )
            LOGGER.info(
                "Cassandra source job %s query=%s mode=%s watermark=%s",
                job.name,
                query,
                mode,
                watermark,
            )

            statement = session.prepare(query) if params is not None else query
            rows = session.execute(statement, params) if params is not None else session.execute(statement)
            seen = 0
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
                yield record
                seen += 1
                if max_records and seen >= max_records:
                    break
        finally:
            session.shutdown()
            cluster.shutdown()
