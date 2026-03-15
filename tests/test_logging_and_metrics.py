from __future__ import annotations

import json
import logging

from migration.logging_utils import configure_logging
from migration.metrics import MetricsCollector


def test_configure_logging_json_includes_static_fields(capsys) -> None:
    try:
        configure_logging(
            "INFO",
            "json",
            static_fields={"pipeline": "v1", "environment": "test"},
        )

        logger = logging.getLogger("test.logger")
        logger.info("structured message", extra={"mapping": "users->Users"})

        payload = json.loads(capsys.readouterr().err.strip())
        assert payload["message"] == "structured message"
        assert payload["pipeline"] == "v1"
        assert payload["environment"] == "test"
        assert payload["mapping"] == "users->Users"
    finally:
        configure_logging("INFO", "text")


def test_metrics_collector_writes_prometheus_snapshot(tmp_path) -> None:
    path = tmp_path / "metrics.prom"
    collector = MetricsCollector(
        str(path),
        output_format="prometheus",
        static_labels={"pipeline": "v1"},
    )
    collector.gauge(
        "migration_v1_docs_seen",
        12,
        labels={"source_container": "users"},
        description="Docs seen.",
    )
    collector.observe(
        "migration_v1_mapping_duration_seconds",
        4.5,
        labels={"source_container": "users"},
        description="Mapping duration.",
    )
    collector.flush()

    output = path.read_text(encoding="utf-8")
    assert "migration_v1_docs_seen" in output
    assert 'pipeline="v1"' in output
    assert "migration_v1_mapping_duration_seconds_count" in output
