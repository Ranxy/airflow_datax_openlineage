from __future__ import annotations

import logging

from openlineage.client.event_v2 import InputDataset, OutputDataset

from airflow_provider_datax_openlineage.lineage.plugins.base import BaseDataXLineagePlugin
from airflow_provider_datax_openlineage.lineage.plugins.mysql import MysqlLineagePlugin
from airflow_provider_datax_openlineage.lineage.plugins.postgres import PostgresLineagePlugin
from airflow_provider_datax_openlineage.lineage.plugins.starrocks import StarrocksLineagePlugin

log = logging.getLogger(__name__)

# Built-in plugins, registered by default.
_BUILTIN_PLUGINS: list[type[BaseDataXLineagePlugin]] = [
    MysqlLineagePlugin,
    PostgresLineagePlugin,
    StarrocksLineagePlugin,
]


class LineageParser:
    """Parses a DataX job config and extracts OpenLineage InputDataset / OutputDataset."""

    def __init__(self, extra_plugins: list[BaseDataXLineagePlugin] | None = None) -> None:
        self._reader_registry: dict[str, BaseDataXLineagePlugin] = {}
        self._writer_registry: dict[str, BaseDataXLineagePlugin] = {}

        # Register built-in plugins
        for plugin_cls in _BUILTIN_PLUGINS:
            self.register(plugin_cls())

        # Register any user-supplied plugins
        if extra_plugins:
            for plugin in extra_plugins:
                self.register(plugin)

    def register(self, plugin: BaseDataXLineagePlugin) -> None:
        for name in plugin.reader_names():
            self._reader_registry[name] = plugin
        for name in plugin.writer_names():
            self._writer_registry[name] = plugin

    def parse(
        self, job_config: dict
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        """Parse a full DataX job config dict.

        Args:
            job_config: The parsed JSON of a DataX job file (top-level has 'job' key).

        Returns:
            A tuple of (input_datasets, output_datasets).
        """
        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []

        content_list = job_config.get("job", {}).get("content", [])

        for content in content_list:
            # --- reader → InputDataset ---
            reader = content.get("reader", {})
            reader_name = reader.get("name", "")
            reader_config = reader.get("parameter", {})

            plugin = self._reader_registry.get(reader_name)
            if plugin:
                for ds in plugin.parse_datasets(reader_name, reader_config):
                    inputs.append(
                        InputDataset(
                            namespace=ds.namespace, name=ds.name, facets=ds.facets
                        )
                    )
            elif reader_name:
                log.warning(
                    "No lineage plugin registered for DataX reader: %s", reader_name
                )

            # --- writer → OutputDataset ---
            writer = content.get("writer", {})
            writer_name = writer.get("name", "")
            writer_config = writer.get("parameter", {})

            plugin = self._writer_registry.get(writer_name)
            if plugin:
                for ds in plugin.parse_datasets(writer_name, writer_config):
                    outputs.append(
                        OutputDataset(
                            namespace=ds.namespace, name=ds.name, facets=ds.facets
                        )
                    )
            elif writer_name:
                log.warning(
                    "No lineage plugin registered for DataX writer: %s", writer_name
                )

        return inputs, outputs
