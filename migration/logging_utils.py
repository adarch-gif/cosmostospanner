from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any


_STANDARD_LOG_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def build_log_context(
    *,
    pipeline: str,
    deployment_environment: str,
    run_id: str = "",
    worker_id: str = "",
) -> dict[str, str]:
    context = {
        "pipeline": str(pipeline),
        "environment": str(deployment_environment),
    }
    if run_id:
        context["run_id"] = str(run_id)
    if worker_id:
        context["worker_id"] = str(worker_id)
    return context


class _StaticContextFilter(logging.Filter):
    def __init__(self, static_fields: dict[str, str]) -> None:
        super().__init__()
        self._static_fields = dict(static_fields)

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in self._static_fields.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class _BaseStructuredFormatter(logging.Formatter):
    def _record_timestamp(self, record: logging.LogRecord) -> str:
        return datetime.fromtimestamp(record.created, tz=UTC).isoformat()

    def _record_fields(self, record: logging.LogRecord) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key in _STANDARD_LOG_RECORD_FIELDS or key.startswith("_"):
                continue
            if callable(value):
                continue
            fields[str(key)] = value
        return fields


class _TextFormatter(_BaseStructuredFormatter):
    def format(self, record: logging.LogRecord) -> str:
        base = (
            f"{self._record_timestamp(record)} "
            f"{record.levelname} "
            f"{record.name}"
        )
        fields = self._record_fields(record)
        if fields:
            rendered_fields = " ".join(
                f"{key}={value}" for key, value in sorted(fields.items())
            )
            base = f"{base} [{rendered_fields}]"
        message = record.getMessage()
        rendered = f"{base} - {message}"
        if record.exc_info:
            rendered = f"{rendered}\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            rendered = f"{rendered}\n{self.formatStack(record.stack_info)}"
        return rendered


class _JsonFormatter(_BaseStructuredFormatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self._record_timestamp(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(self._record_fields(record))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def configure_logging(
    level: str = "INFO",
    log_format: str = "text",
    static_fields: dict[str, str] | None = None,
) -> None:
    normalized_format = str(log_format or "text").lower()
    if normalized_format not in {"text", "json"}:
        raise ValueError("log_format must be one of: text, json")

    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter() if normalized_format == "json" else _TextFormatter())

    context_filter = _StaticContextFilter(
        {
            "configured_at": _utc_timestamp(),
            **{str(key): str(value) for key, value in (static_fields or {}).items()},
        }
    )
    handler.addFilter(context_filter)

    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        handlers=[handler],
        force=True,
    )
