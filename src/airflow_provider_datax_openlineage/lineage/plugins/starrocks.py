from __future__ import annotations

import logging
from urllib.parse import urlparse

from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import schema_dataset

from airflow_provider_datax_openlineage.lineage.plugins.base import BaseDataXLineagePlugin

log = logging.getLogger(__name__)


def _parse_jdbc_url(jdbc_url: str) -> tuple[str, int, str]:
    """Extract host, port, database from a StarRocks (MySQL-protocol) JDBC URL.

    Handles formats like:
        jdbc:mysql://host:9030/database
        jdbc:mysql://host:9030/database?useUnicode=true
        jdbc:mysql://host/database
    """
    url = jdbc_url.removeprefix("jdbc:")
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 9030
    database = parsed.path.lstrip("/").split("?")[0] if parsed.path else ""
    return host, port, database


def _build_schema_facet(
    columns: list[str],
) -> schema_dataset.SchemaDatasetFacet | None:
    """Build a SchemaDatasetFacet from a DataX column list.

    Returns None if columns is empty or contains only '*' (select all).
    """
    if not columns or columns == ["*"]:
        return None
    fields = [schema_dataset.SchemaDatasetFacetFields(name=col) for col in columns]
    return schema_dataset.SchemaDatasetFacet(fields=fields)


class StarrocksLineagePlugin(BaseDataXLineagePlugin):
    """Lineage plugin for starrocksreader / starrockswriter.

    The StarRocks writer addresses its target through ``connection[].jdbcUrl``
    (a single URL string) plus ``selectedDatabase``; the reader uses the same
    shape as the other rdbms readers (a URL list). Both are handled here.
    """

    def reader_names(self) -> list[str]:
        return ["starrocksreader"]

    def writer_names(self) -> list[str]:
        return ["starrockswriter"]

    def parse_datasets(self, plugin_name: str, plugin_config: dict) -> list[Dataset]:
        datasets: list[Dataset] = []
        columns = plugin_config.get("column", [])
        schema_facet = _build_schema_facet(columns)
        facets = {"schema": schema_facet} if schema_facet else {}

        for conn in plugin_config.get("connection", []):
            jdbc_urls = conn.get("jdbcUrl", [])
            if isinstance(jdbc_urls, str):
                jdbc_urls = [jdbc_urls]
            tables = conn.get("table", [])
            if isinstance(tables, str):
                tables = [tables]
            selected_database = conn.get("selectedDatabase", "")

            for jdbc_url in jdbc_urls:
                try:
                    host, port, database = _parse_jdbc_url(jdbc_url)
                except Exception:
                    log.warning("Failed to parse StarRocks JDBC URL: %s", jdbc_url)
                    continue

                if not database:
                    database = selected_database

                namespace = f"starrocks://{host}:{port}"
                for table in tables:
                    name = f"{database}.{table}" if database else table
                    datasets.append(Dataset(namespace=namespace, name=name, facets=facets))

        if not datasets:
            log.warning(
                "No StarRocks datasets extracted from %s config; a jdbcUrl is required",
                plugin_name,
            )

        return datasets
