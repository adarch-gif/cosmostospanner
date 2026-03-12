from __future__ import annotations

import logging

import pytest

from migration.retry_utils import RetryPolicy, run_with_retry


def test_run_with_retry_eventual_success(monkeypatch) -> None:
    calls = {"count": 0}
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr("migration.retry_utils.time.sleep", fake_sleep)

    def op() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise RuntimeError("transient")
        return "ok"

    policy = RetryPolicy(
        attempts=4,
        initial_delay_seconds=0.1,
        max_delay_seconds=1.0,
        backoff_multiplier=2.0,
        jitter_seconds=0.0,
    )
    result = run_with_retry(
        op,
        operation_name="test_op",
        policy=policy,
        logger=logging.getLogger("test"),
        retriable_exceptions=(RuntimeError,),
    )
    assert result == "ok"
    assert calls["count"] == 3
    assert slept == [0.1, 0.2]


def test_run_with_retry_raises_after_attempts() -> None:
    policy = RetryPolicy(
        attempts=2,
        initial_delay_seconds=0.0,
        max_delay_seconds=0.0,
        backoff_multiplier=2.0,
        jitter_seconds=0.0,
    )
    with pytest.raises(RuntimeError, match="permanent"):
        run_with_retry(
            lambda: (_ for _ in ()).throw(RuntimeError("permanent")),
            operation_name="always_fail",
            policy=policy,
            logger=logging.getLogger("test"),
            retriable_exceptions=(RuntimeError,),
        )

