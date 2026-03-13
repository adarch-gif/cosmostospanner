from __future__ import annotations

import os

import pytest

from scripts import preflight, v2_preflight


def _require_integration_config(env_var: str) -> str:
    if os.getenv("RUN_LIVE_INTEGRATION_TESTS", "").lower() != "true":
        pytest.skip("Set RUN_LIVE_INTEGRATION_TESTS=true to enable live integration tests.")

    config_path = os.getenv(env_var, "")
    if not config_path:
        pytest.skip(f"Set {env_var} to a live integration config file.")
    return config_path


@pytest.mark.integration
def test_v1_preflight_live_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = _require_integration_config("COSMOS_TO_SPANNER_V1_INTEGRATION_CONFIG")
    monkeypatch.setattr(
        "sys.argv",
        ["preflight.py", "--config", config_path, "--check-source"],
    )
    assert preflight.main() == 0


@pytest.mark.integration
def test_v2_preflight_live_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = _require_integration_config("COSMOS_TO_SPANNER_V2_INTEGRATION_CONFIG")
    monkeypatch.setattr(
        "sys.argv",
        ["v2_preflight.py", "--config", config_path, "--check-sources"],
    )
    assert v2_preflight.main() == 0
