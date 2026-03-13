from __future__ import annotations

from typing import Iterable, Protocol

from migration.resume import StreamResumeState
from migration_v2.config import CassandraJobConfig, MongoJobConfig
from migration_v2.models import CanonicalRecord


class SourceAdapter(Protocol):
    def iter_records(
        self,
        job: MongoJobConfig | CassandraJobConfig,
        *,
        mode: str,
        watermark: object | None,
        max_records: int | None,
        shard_index: int | None = None,
        shard_count: int = 1,
        resume_state: StreamResumeState | None = None,
    ) -> Iterable[CanonicalRecord]:
        ...

