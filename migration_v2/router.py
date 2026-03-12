from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from migration_v2.config import RoutingConfig


class RouteDestination(str, Enum):
    FIRESTORE = "firestore"
    SPANNER = "spanner"
    REJECT = "reject"


@dataclass(frozen=True)
class RouteDecision:
    destination: RouteDestination
    reason: str


class SizeRouter:
    def __init__(self, config: RoutingConfig) -> None:
        self._config = config

    def decide(self, payload_size_bytes: int) -> RouteDecision:
        effective_size = payload_size_bytes + self._config.payload_size_overhead_bytes
        if effective_size < self._config.firestore_lt_bytes:
            return RouteDecision(
                destination=RouteDestination.FIRESTORE,
                reason=(
                    f"effective_size={effective_size} < firestore_lt_bytes="
                    f"{self._config.firestore_lt_bytes}"
                ),
            )
        if effective_size <= self._config.spanner_max_payload_bytes:
            return RouteDecision(
                destination=RouteDestination.SPANNER,
                reason=(
                    f"effective_size={effective_size} <= spanner_max_payload_bytes="
                    f"{self._config.spanner_max_payload_bytes}"
                ),
            )
        return RouteDecision(
            destination=RouteDestination.REJECT,
            reason=(
                f"effective_size={effective_size} exceeds spanner_max_payload_bytes="
                f"{self._config.spanner_max_payload_bytes}"
            ),
        )

