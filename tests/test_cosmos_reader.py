from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

azure_module = ModuleType("azure")
cosmos_module = ModuleType("azure.cosmos")
cosmos_module.CosmosClient = object
azure_module.cosmos = cosmos_module
sys.modules.setdefault("azure", azure_module)
sys.modules.setdefault("azure.cosmos", cosmos_module)

from migration.config import CosmosConfig
from migration.cosmos_reader import CosmosReader
from migration.resume import StreamResumeState, build_resume_scope
from migration.retry_utils import RetryPolicy


class _FakePager:
    def __init__(
        self,
        pages: list[list[dict[str, Any]]],
        *,
        start_index: int,
        fail_on_index: int | None,
        failure_state: dict[str, bool],
    ) -> None:
        self._pages = pages
        self._index = start_index
        self._fail_on_index = fail_on_index
        self._failure_state = failure_state
        self.continuation_token: int | None = start_index

    def __iter__(self) -> "_FakePager":
        return self

    def __next__(self) -> list[dict[str, Any]]:
        if self._fail_on_index is not None and self._index == self._fail_on_index:
            if not self._failure_state["raised"]:
                self._failure_state["raised"] = True
                raise RuntimeError("transient cosmos page failure")
        if self._index >= len(self._pages):
            self.continuation_token = None
            raise StopIteration
        page = self._pages[self._index]
        self._index += 1
        self.continuation_token = self._index if self._index < len(self._pages) else None
        return page


class _FakeItemPaged:
    def __init__(
        self,
        pages: list[list[dict[str, Any]]],
        *,
        fail_on_index: int | None,
        failure_state: dict[str, bool],
    ) -> None:
        self._pages = pages
        self._fail_on_index = fail_on_index
        self._failure_state = failure_state

    def by_page(self, continuation_token: int | None = None) -> _FakePager:
        return _FakePager(
            self._pages,
            start_index=continuation_token or 0,
            fail_on_index=self._fail_on_index,
            failure_state=self._failure_state,
        )


class _FakeContainer:
    def __init__(self) -> None:
        self.query_calls = 0
        self._failure_state = {"raised": False}

    def query_items(self, **_: Any) -> _FakeItemPaged:
        self.query_calls += 1
        return _FakeItemPaged(
            [
                [{"id": "1"}, {"id": "2"}],
                [{"id": "3"}],
            ],
            fail_on_index=1,
            failure_state=self._failure_state,
        )


class _FakeDatabase:
    def __init__(self) -> None:
        self.container = _FakeContainer()

    def get_container_client(self, _container_name: str) -> _FakeContainer:
        return self.container


class _FakeCosmosClient:
    def __init__(self, *, url: str, credential: str) -> None:
        del url, credential
        self.database = _FakeDatabase()

    def get_database_client(self, _database_name: str) -> _FakeDatabase:
        return self.database


def test_iter_documents_resumes_from_continuation_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("migration.cosmos_reader.CosmosClient", _FakeCosmosClient)

    reader = CosmosReader(
        CosmosConfig(
            endpoint="https://example.documents.azure.com:443/",
            key="key",
            database="db",
        ),
        retry_policy=RetryPolicy(
            attempts=3,
            initial_delay_seconds=0.0,
            max_delay_seconds=0.0,
            backoff_multiplier=1.0,
            jitter_seconds=0.0,
        ),
    )

    docs = list(
        reader.iter_documents(
            container_name="users",
            query="SELECT * FROM c",
            page_size=2,
        )
    )

    assert [doc["id"] for doc in docs] == ["1", "2", "3"]
    assert reader._database.container.query_calls == 2


def test_iter_documents_reuses_persisted_page_start_token_and_skips_within_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("migration.cosmos_reader.CosmosClient", _FakeCosmosClient)

    reader = CosmosReader(
        CosmosConfig(
            endpoint="https://example.documents.azure.com:443/",
            key="key",
            database="db",
        ),
        retry_policy=RetryPolicy(
            attempts=2,
            initial_delay_seconds=0.0,
            max_delay_seconds=0.0,
            backoff_multiplier=1.0,
            jitter_seconds=0.0,
        ),
    )
    resume_state = StreamResumeState(
        last_source_key="2",
        last_watermark=None,
        emitted_count=2,
        page_start_token=0,
        scope=build_resume_scope(
            container_name="users",
            query="SELECT * FROM c",
            parameters=[],
            page_size=2,
            max_docs=None,
        ),
    )

    docs = list(
        reader.iter_documents(
            container_name="users",
            query="SELECT * FROM c",
            page_size=2,
            resume_state=resume_state,
        )
    )

    assert [doc["id"] for doc in docs] == ["3"]


def test_iter_documents_capped_runs_resume_without_skipping_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("migration.cosmos_reader.CosmosClient", _FakeCosmosClient)

    reader = CosmosReader(
        CosmosConfig(
            endpoint="https://example.documents.azure.com:443/",
            key="key",
            database="db",
        ),
        retry_policy=RetryPolicy(
            attempts=3,
            initial_delay_seconds=0.0,
            max_delay_seconds=0.0,
            backoff_multiplier=1.0,
            jitter_seconds=0.0,
        ),
    )
    resume_state = StreamResumeState()

    first = list(
        reader.iter_documents(
            container_name="users",
            query="SELECT * FROM c",
            page_size=2,
            max_docs=1,
            resume_state=resume_state,
        )
    )
    second = list(
        reader.iter_documents(
            container_name="users",
            query="SELECT * FROM c",
            page_size=2,
            max_docs=1,
            resume_state=resume_state,
        )
    )
    third = list(
        reader.iter_documents(
            container_name="users",
            query="SELECT * FROM c",
            page_size=2,
            max_docs=1,
            resume_state=resume_state,
        )
    )

    assert [doc["id"] for doc in first] == ["1"]
    assert [doc["id"] for doc in second] == ["2"]
    assert [doc["id"] for doc in third] == ["3"]
    assert resume_state.last_source_key == "3"
