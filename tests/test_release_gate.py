from __future__ import annotations

from types import SimpleNamespace

import pytest

from migration.release_gate import (
    RELEASE_GATE_STATUS_PASSED,
    ReleaseGateStore,
    build_release_gate_record,
    enforce_stage_rehearsal_or_raise,
    logical_fingerprint_v1,
    make_release_gate_key,
    verify_stage_release_gate,
)
from tests.test_config import _base_config, _write_config
from migration.config import load_config


def test_release_gate_store_roundtrip_and_verify(tmp_path) -> None:
    path = tmp_path / "release-gate.json"
    store = ReleaseGateStore(str(path))
    record = build_release_gate_record(
        pipeline="v1",
        environment="stage",
        scope="cutover-1",
        status=RELEASE_GATE_STATUS_PASSED,
        logical_fingerprint="abc123",
        checks=["preflight", "validation"],
        attested_at="2026-03-13T00:00:00+00:00",
    )
    store.set(make_release_gate_key("v1", "stage", "cutover-1"), record)
    store.flush()

    reloaded = ReleaseGateStore(str(path))
    assert (
        verify_stage_release_gate(
            store=reloaded,
            pipeline="v1",
            scope="cutover-1",
            logical_fingerprint="abc123",
            max_age_hours=48,
            now=record_attested_plus(hours=1),
        )
        is None
    )


def test_verify_stage_release_gate_rejects_stale_record(tmp_path) -> None:
    path = tmp_path / "release-gate.json"
    store = ReleaseGateStore(str(path))
    store.set(
        make_release_gate_key("v2", "stage", "cutover-2"),
        build_release_gate_record(
            pipeline="v2",
            environment="stage",
            scope="cutover-2",
            status=RELEASE_GATE_STATUS_PASSED,
            logical_fingerprint="fp",
            checks=["preflight", "validation"],
            attested_at="2026-03-10T00:00:00+00:00",
        ),
    )
    store.flush()

    error = verify_stage_release_gate(
        store=ReleaseGateStore(str(path)),
        pipeline="v2",
        scope="cutover-2",
        logical_fingerprint="fp",
        max_age_hours=24,
        now=record_attested_plus(days=4),
    )
    assert error is not None
    assert "older than" in error


def test_enforce_stage_rehearsal_raises_when_missing_gate(tmp_path) -> None:
    runtime = SimpleNamespace(
        deployment_environment="prod",
        release_gate_file=str(tmp_path / "release-gate.json"),
        release_gate_scope="cutover-3",
        release_gate_max_age_hours=24,
        require_stage_rehearsal_for_prod=True,
    )
    with pytest.raises(RuntimeError, match="Missing successful stage release-gate record"):
        enforce_stage_rehearsal_or_raise(
            runtime=runtime,
            pipeline="v1",
            logical_fingerprint="fp",
        )


def test_logical_fingerprint_v1_changes_with_mapping_selection(tmp_path) -> None:
    cfg = _base_config()
    cfg["mappings"].append(
        {
            "source_container": "orders",
            "target_table": "Orders",
            "key_columns": ["order_id"],
            "columns": [{"target": "order_id", "source": "id"}],
        }
    )
    config = load_config(_write_config(tmp_path, cfg))
    first = logical_fingerprint_v1(config, [config.mappings[0]])
    second = logical_fingerprint_v1(config, config.mappings)
    assert first != second


def record_attested_plus(*, hours: int = 0, days: int = 0):
    from datetime import datetime, timedelta, timezone

    return datetime(2026, 3, 13, tzinfo=timezone.utc) + timedelta(hours=hours, days=days)
