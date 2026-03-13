from __future__ import annotations

import json
import logging
from typing import Any, Iterable

from azure.cosmos import CosmosClient

from migration.config import CosmosConfig
from migration.resume import StreamResumeState, build_resume_scope
from migration.retry_utils import RetryPolicy, iter_with_retry, run_with_retry

LOGGER = logging.getLogger("cosmos_reader")


class CosmosReader:
    def __init__(self, config: CosmosConfig, retry_policy: RetryPolicy) -> None:
        self._client = CosmosClient(url=config.endpoint, credential=config.key)
        self._database = self._client.get_database_client(config.database)
        self._retry_policy = retry_policy

    def iter_documents(
        self,
        container_name: str,
        query: str,
        parameters: list[dict[str, Any]] | None = None,
        page_size: int = 200,
        max_docs: int | None = None,
        resume_state: StreamResumeState | None = None,
    ) -> Iterable[dict[str, Any]]:
        container = run_with_retry(
            lambda: self._database.get_container_client(container_name),
            operation_name=f"get_container_client:{container_name}",
            policy=self._retry_policy,
            logger=LOGGER,
        )
        seen = 0
        state = resume_state or StreamResumeState()
        state.ensure_scope(
            build_resume_scope(
                container_name=container_name,
                query=query,
                parameters=parameters or [],
                page_size=page_size,
                max_docs=max_docs,
            )
        )
        continuation_token = state.page_start_token

        def open_stream() -> Iterable[dict[str, Any]]:
            nonlocal continuation_token, seen
            iterator = run_with_retry(
                lambda: container.query_items(
                    query=query,
                    parameters=parameters or [],
                    enable_cross_partition_query=True,
                    max_item_count=page_size,
                ),
                operation_name=f"query_items:{container_name}",
                policy=self._retry_policy,
                logger=LOGGER,
            )
            pager = iterator.by_page(continuation_token=continuation_token)
            current_page_token = continuation_token
            for page in pager:
                current_page = list(page)
                next_token = getattr(pager, "continuation_token", None)
                state.set_page_start_token(current_page_token)
                for item in current_page:
                    source_key = str(item.get("id", "")) or json.dumps(
                        item,
                        sort_keys=True,
                        ensure_ascii=False,
                        default=str,
                    )
                    watermark = item.get("_ts")
                    if state.should_skip(
                        source_key=source_key,
                        watermark=watermark,
                        compare_watermarks=_compare_watermarks,
                    ):
                        continue
                    state.mark(source_key=source_key, watermark=watermark)
                    if max_docs and seen >= max_docs:
                        return
                    seen += 1
                    yield item
                continuation_token = next_token
                current_page_token = next_token
                if max_docs and seen >= max_docs:
                    return

        def on_retry(error: BaseException, attempt: int) -> None:
            LOGGER.warning(
                "Resuming Cosmos source stream for %s from continuation token after attempt %s due to %s",
                container_name,
                attempt,
                error,
            )

        yield from iter_with_retry(
            open_stream,
            operation_name=f"iter_documents:{container_name}",
            policy=self._retry_policy,
            logger=LOGGER,
            on_retry=on_retry,
        )

    def count_documents(
        self,
        container_name: str,
        query: str = "SELECT VALUE COUNT(1) FROM c",
        parameters: list[dict[str, Any]] | None = None,
    ) -> int:
        container = run_with_retry(
            lambda: self._database.get_container_client(container_name),
            operation_name=f"get_container_client:{container_name}",
            policy=self._retry_policy,
            logger=LOGGER,
        )
        rows = run_with_retry(
            lambda: container.query_items(
                query=query,
                parameters=parameters or [],
                enable_cross_partition_query=True,
                max_item_count=1,
            ),
            operation_name=f"count_query:{container_name}",
            policy=self._retry_policy,
            logger=LOGGER,
        )
        return run_with_retry(
            lambda: int(next(iter(rows), 0) or 0),
            operation_name=f"count_result:{container_name}",
            policy=self._retry_policy,
            logger=LOGGER,
        )


def _compare_watermarks(left: Any, right: Any) -> int:
    if left is None and right is None:
        return 0
    if left is None:
        return -1
    if right is None:
        return 1
    left_int = int(left)
    right_int = int(right)
    if left_int < right_int:
        return -1
    if left_int > right_int:
        return 1
    return 0
