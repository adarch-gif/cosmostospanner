from __future__ import annotations

import os
import time
from pathlib import Path


class FileLock:
    def __init__(
        self,
        lock_path: Path,
        *,
        timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.1,
        stale_lock_seconds: float = 300.0,
    ) -> None:
        self._path = lock_path
        self._timeout_seconds = timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._stale_lock_seconds = stale_lock_seconds
        self._fd: int | None = None

    def _try_break_stale_lock(self) -> None:
        try:
            stat = self._path.stat()
        except FileNotFoundError:
            return
        age_seconds = max(0.0, time.time() - stat.st_mtime)
        if age_seconds >= self._stale_lock_seconds:
            try:
                self._path.unlink()
            except FileNotFoundError:
                return

    def acquire(self) -> None:
        start = time.monotonic()
        while True:
            try:
                self._fd = os.open(str(self._path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(self._fd, f"{os.getpid()}".encode("utf-8"))
                return
            except FileExistsError:
                self._try_break_stale_lock()
                if time.monotonic() - start >= self._timeout_seconds:
                    raise TimeoutError(
                        f"Timed out acquiring state lock: {self._path}"
                    ) from None
                time.sleep(self._poll_interval_seconds)

    def release(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        try:
            self._path.unlink()
        except FileNotFoundError:
            return

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
