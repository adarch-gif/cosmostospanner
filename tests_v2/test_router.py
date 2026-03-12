from __future__ import annotations

from migration_v2.config import RoutingConfig
from migration_v2.router import RouteDestination, SizeRouter


def test_size_router_routes_below_threshold_to_firestore() -> None:
    router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1_048_576,
            spanner_max_payload_bytes=10_000_000,
            payload_size_overhead_bytes=0,
        )
    )
    decision = router.decide(1000)
    assert decision.destination == RouteDestination.FIRESTORE


def test_size_router_routes_threshold_to_spanner() -> None:
    router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1_048_576,
            spanner_max_payload_bytes=10_000_000,
            payload_size_overhead_bytes=0,
        )
    )
    decision = router.decide(1_048_576)
    assert decision.destination == RouteDestination.SPANNER


def test_size_router_rejects_above_spanner_max() -> None:
    router = SizeRouter(
        RoutingConfig(
            firestore_lt_bytes=1_048_576,
            spanner_max_payload_bytes=2_000_000,
            payload_size_overhead_bytes=0,
        )
    )
    decision = router.decide(2_000_001)
    assert decision.destination == RouteDestination.REJECT

