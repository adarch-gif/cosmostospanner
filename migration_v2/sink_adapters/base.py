from __future__ import annotations

from typing import Protocol

from migration_v2.models import CanonicalRecord


class SinkAdapter(Protocol):
    def upsert(self, record: CanonicalRecord) -> None:
        ...

    def delete(self, record: CanonicalRecord) -> None:
        ...

    def preflight_check(self) -> tuple[bool, str]:
        ...

