from __future__ import annotations

from typing import Any, Iterable

from azure.cosmos import CosmosClient

from migration.config import CosmosConfig


class CosmosReader:
    def __init__(self, config: CosmosConfig) -> None:
        self._client = CosmosClient(url=config.endpoint, credential=config.key)
        self._database = self._client.get_database_client(config.database)

    def iter_documents(
        self,
        container_name: str,
        query: str,
        parameters: list[dict[str, Any]] | None = None,
        page_size: int = 200,
        max_docs: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        container = self._database.get_container_client(container_name)
        iterator = container.query_items(
            query=query,
            parameters=parameters or [],
            enable_cross_partition_query=True,
            max_item_count=page_size,
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
        container = self._database.get_container_client(container_name)
        rows = container.query_items(
            query=query,
            parameters=parameters or [],
            enable_cross_partition_query=True,
            max_item_count=1,
        )
        first = next(iter(rows), 0)
        return int(first or 0)

