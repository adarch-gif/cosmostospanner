from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any


_METRIC_NAME_RE = re.compile(r"[^a-zA-Z0-9_:]")
_LABEL_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _sanitize_metric_name(name: str) -> str:
    normalized = _METRIC_NAME_RE.sub("_", str(name).strip())
    if not normalized:
        raise ValueError("Metric name must not be empty.")
    if normalized[0].isdigit():
        normalized = f"metric_{normalized}"
    return normalized


def _sanitize_label_name(name: str) -> str:
    normalized = _LABEL_NAME_RE.sub("_", str(name).strip())
    if not normalized:
        raise ValueError("Metric label name must not be empty.")
    if normalized[0].isdigit():
        normalized = f"label_{normalized}"
    return normalized


def _normalize_labels(
    static_labels: dict[str, str],
    labels: dict[str, Any] | None,
) -> tuple[tuple[str, str], ...]:
    merged: dict[str, str] = dict(static_labels)
    for key, value in (labels or {}).items():
        if value is None or value == "":
            continue
        merged[_sanitize_label_name(key)] = str(value)
    return tuple(sorted(merged.items()))


def _render_prometheus_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    parts = []
    for key, value in labels:
        escaped = value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
        parts.append(f'{key}="{escaped}"')
    return "{" + ",".join(parts) + "}"


@dataclass(frozen=True)
class _MetricSample:
    name: str
    metric_type: str
    description: str
    labels: tuple[tuple[str, str], ...]
    value: float


class MetricsCollector:
    def __init__(
        self,
        file_path: str = "",
        *,
        output_format: str = "prometheus",
        static_labels: dict[str, Any] | None = None,
    ) -> None:
        normalized_format = str(output_format or "prometheus").lower()
        if normalized_format not in {"prometheus", "json"}:
            raise ValueError("metrics output_format must be one of: prometheus, json")

        self.file_path = Path(file_path) if file_path else None
        self.output_format = normalized_format
        self.static_labels = {
            _sanitize_label_name(key): str(value)
            for key, value in (static_labels or {}).items()
            if value is not None and value != ""
        }
        self._lock = RLock()
        self._descriptions: dict[str, str] = {}
        self._types: dict[str, str] = {}
        self._values: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}

    @property
    def enabled(self) -> bool:
        return self.file_path is not None

    def _register_metric(self, name: str, metric_type: str, description: str) -> str:
        normalized_name = _sanitize_metric_name(name)
        existing_type = self._types.get(normalized_name)
        if existing_type and existing_type != metric_type:
            raise ValueError(
                f"Metric {normalized_name} already registered as {existing_type}, not {metric_type}."
            )
        self._types[normalized_name] = metric_type
        self._descriptions.setdefault(normalized_name, description.strip() or normalized_name)
        return normalized_name

    def gauge(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, Any] | None = None,
        description: str = "",
    ) -> None:
        normalized_name = self._register_metric(name, "gauge", description)
        label_key = _normalize_labels(self.static_labels, labels)
        with self._lock:
            self._values[(normalized_name, label_key)] = float(value)

    def increment(
        self,
        name: str,
        amount: float = 1.0,
        *,
        labels: dict[str, Any] | None = None,
        description: str = "",
    ) -> None:
        normalized_name = self._register_metric(name, "counter", description)
        label_key = _normalize_labels(self.static_labels, labels)
        with self._lock:
            current = self._values.get((normalized_name, label_key), 0.0)
            self._values[(normalized_name, label_key)] = current + float(amount)

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, Any] | None = None,
        description: str = "",
    ) -> None:
        base_description = description.strip() or name
        self.increment(
            f"{name}_count",
            1.0,
            labels=labels,
            description=f"Observation count for {base_description}.",
        )
        self.increment(
            f"{name}_sum",
            value,
            labels=labels,
            description=f"Observation sum for {base_description}.",
        )
        max_metric_name = self._register_metric(
            f"{name}_max",
            "gauge",
            f"Maximum observed value for {base_description}.",
        )
        label_key = _normalize_labels(self.static_labels, labels)
        with self._lock:
            current = self._values.get((max_metric_name, label_key))
            candidate = float(value)
            if current is None or candidate > current:
                self._values[(max_metric_name, label_key)] = candidate

    def _snapshot(self) -> list[_MetricSample]:
        samples: list[_MetricSample] = []
        for (name, labels), value in sorted(self._values.items()):
            samples.append(
                _MetricSample(
                    name=name,
                    metric_type=self._types[name],
                    description=self._descriptions.get(name, name),
                    labels=labels,
                    value=value,
                )
            )
        return samples

    def flush(self) -> None:
        if self.file_path is None:
            return
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            payload = self._render_json() if self.output_format == "json" else self._render_prometheus()
        temp_path = self.file_path.with_suffix(self.file_path.suffix + ".tmp")
        temp_path.write_text(payload, encoding="utf-8")
        temp_path.replace(self.file_path)

    def _render_json(self) -> str:
        return json.dumps(
            {
                "generated_at": _utc_now_iso(),
                "metrics": [
                    {
                        "name": sample.name,
                        "type": sample.metric_type,
                        "description": sample.description,
                        "labels": dict(sample.labels),
                        "value": sample.value,
                    }
                    for sample in self._snapshot()
                ],
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )

    def _render_prometheus(self) -> str:
        lines: list[str] = [f"# generated_at {_utc_now_iso()}"]
        last_name = ""
        for sample in self._snapshot():
            if sample.name != last_name:
                lines.append(f"# HELP {sample.name} {sample.description}")
                lines.append(f"# TYPE {sample.name} {sample.metric_type}")
                last_name = sample.name
            lines.append(
                f"{sample.name}{_render_prometheus_labels(sample.labels)} {sample.value}"
            )
        return "\n".join(lines) + "\n"
