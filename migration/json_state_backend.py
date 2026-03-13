from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from migration.file_lock import FileLock

GCS_PREFIX = "gs://"


@dataclass(frozen=True)
class GcsObjectRef:
    bucket: str
    blob: str


def is_gcs_path(location: str) -> bool:
    return location.startswith(GCS_PREFIX)


def parse_gcs_path(location: str) -> GcsObjectRef:
    if not is_gcs_path(location):
        raise ValueError(f"Not a GCS path: {location}")
    bucket_and_blob = location[len(GCS_PREFIX) :]
    bucket, _, blob = bucket_and_blob.partition("/")
    if not bucket or not blob:
        raise ValueError(
            "GCS state paths must use the form gs://<bucket>/<object>. "
            f"Received: {location!r}"
        )
    return GcsObjectRef(bucket=bucket, blob=blob)


class JsonStateBackend:
    def __init__(self, location: str) -> None:
        self.location = location
        self.path = Path(location) if not is_gcs_path(location) else None
        self.lock_path = (
            self.path.with_suffix(self.path.suffix + ".lock")
            if self.path is not None
            else None
        )
        self.gcs_ref = parse_gcs_path(location) if is_gcs_path(location) else None

    def read_json_object(self) -> dict[str, Any]:
        if self.gcs_ref is not None:
            data, _ = self._read_gcs_object()
            return data
        return self._read_local_object()

    def merge_write_json_object(
        self,
        merge_fn: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        ensure_ascii: bool = False,
    ) -> dict[str, Any]:
        if self.gcs_ref is not None:
            return self._merge_write_gcs_object(merge_fn, ensure_ascii=ensure_ascii)
        return self._merge_write_local_object(merge_fn, ensure_ascii=ensure_ascii)

    def _read_local_object(self) -> dict[str, Any]:
        if self.path is None:
            raise ValueError("Local state backend is not configured.")
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"State file is corrupted: {self.path}. "
                "Restore a valid JSON document or remove the file to reset."
            ) from exc
        if not isinstance(raw, dict):
            raise ValueError(f"State file must contain a JSON object: {self.path}")
        return raw

    def _merge_write_local_object(
        self,
        merge_fn: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        ensure_ascii: bool,
    ) -> dict[str, Any]:
        if self.path is None or self.lock_path is None:
            raise ValueError("Local state backend is not configured.")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(self.lock_path):
            current_data = self._read_local_object()
            merged = merge_fn(current_data)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(
                json.dumps(merged, indent=2, ensure_ascii=ensure_ascii),
                encoding="utf-8",
            )
            tmp.replace(self.path)
            return merged

    def _read_gcs_object(self) -> tuple[dict[str, Any], int | None]:
        if self.gcs_ref is None:
            raise ValueError("GCS state backend is not configured.")
        from google.api_core.exceptions import NotFound
        from google.cloud import storage

        blob = storage.Client().bucket(self.gcs_ref.bucket).blob(self.gcs_ref.blob)
        try:
            blob.reload()
        except NotFound:
            return {}, None

        try:
            raw = json.loads(blob.download_as_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"State file is corrupted: {self.location}. "
                "Restore a valid JSON document or remove the object to reset."
            ) from exc
        if not isinstance(raw, dict):
            raise ValueError(f"State file must contain a JSON object: {self.location}")
        generation = int(blob.generation) if blob.generation is not None else None
        return raw, generation

    def _merge_write_gcs_object(
        self,
        merge_fn: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        ensure_ascii: bool,
    ) -> dict[str, Any]:
        if self.gcs_ref is None:
            raise ValueError("GCS state backend is not configured.")
        from google.api_core.exceptions import PreconditionFailed
        from google.cloud import storage

        client = storage.Client()
        blob = client.bucket(self.gcs_ref.bucket).blob(self.gcs_ref.blob)

        last_error: Exception | None = None
        for _ in range(5):
            current_data, generation = self._read_gcs_object()
            merged = merge_fn(current_data)
            payload = json.dumps(merged, indent=2, ensure_ascii=ensure_ascii)
            try:
                # Generation preconditions provide optimistic concurrency for
                # multi-runner updates without requiring an external lock service.
                if generation is None:
                    blob.upload_from_string(
                        payload,
                        content_type="application/json",
                        if_generation_match=0,
                    )
                else:
                    blob.upload_from_string(
                        payload,
                        content_type="application/json",
                        if_generation_match=generation,
                    )
                return merged
            except PreconditionFailed as exc:
                last_error = exc
                continue

        raise RuntimeError(
            f"Failed to update state object {self.location} after repeated GCS write conflicts."
        ) from last_error
