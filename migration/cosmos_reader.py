from __future__ import annotations

import logging
from typing import Any, Iterable

from azure.cosmos import CosmosClient

from migration.config import CosmosConfig
from migration.retry_utils import RetryPolicy, run_with_retry

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
    ) -> Iterable[dict[str, Any]]:
        container = run_with_retry(
            lambda: self._database.get_container_client(container_name),
            operation_name=f"get_container_client:{container_name}",
            policy=self._retry_policy,
            logger=LOGGER,
        )
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

        seen = 0
        for item in iterator:
            yield item
            seen += 1
            if max_docs and seen >= max_docs:
                break

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
