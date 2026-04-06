from __future__ import annotations

import logging
from urllib.parse import urlparse

from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import schema_dataset

from airflow_provider_datax_openlineage.lineage.plugins.base import BaseDataXLineagePlugin

log = logging.getLogger(__name__)


def _parse_jdbc_url(jdbc_url: str) -> tuple[str, int, str]:
    """Extract host, port, database from a PostgreSQL JDBC URL.

    Handles formats like:
        jdbc:postgresql://host:5432/database
        jdbc:postgresql://host:5432/database?currentSchema=public
        jdbc:postgresql://host/database
    """
    url = jdbc_url.removeprefix("jdbc:")
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    database = parsed.path.lstrip("/").split("?")[0] if parsed.path else ""
    return host, port, database


def _build_schema_facet(
    columns: list[str],
) -> schema_dataset.SchemaDatasetFacet | None:
    """Build a SchemaDatasetFacet from DataX column list.

    Returns None if columns is empty or contains only '*' (select all).
    """
    if not columns or columns == ["*"]:
        return None
    fields = [schema_dataset.SchemaDatasetFacetFields(name=col) for col in columns]
    return schema_dataset.SchemaDatasetFacet(fields=fields)


class PostgresLineagePlugin(BaseDataXLineagePlugin):
    """Lineage plugin for postgresqlreader / postgresqlwriter."""

    def reader_names(self) -> list[str]:
        return ["postgresqlreader"]

    def writer_names(self) -> list[str]:
        return ["postgresqlwriter"]

    def parse_datasets(self, plugin_name: str, plugin_config: dict) -> list[Dataset]:
        datasets: list[Dataset] = []
        columns = plugin_config.get("column", [])
        schema_facet = _build_schema_facet(columns)
        facets = {"schema": schema_facet} if schema_facet else {}

        connections = plugin_config.get("connection", [])
        for conn in connections:
            jdbc_urls = conn.get("jdbcUrl", [])
            if isinstance(jdbc_urls, str):
                jdbc_urls = [jdbc_urls]
            tables = conn.get("table", [])
            if isinstance(tables, str):
                tables = [tables]

            for jdbc_url in jdbc_urls:
                try:
                    host, port, database = _parse_jdbc_url(jdbc_url)
                except Exception:
                    log.warning("Failed to parse JDBC URL: %s", jdbc_url)
                    continue

                namespace = f"postgres://{host}:{port}"
                for table in tables:
                    name = f"{database}.{table}" if database else table
                    datasets.append(Dataset(namespace=namespace, name=name, facets=facets))

        return datasets
