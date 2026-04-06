from __future__ import annotations

from abc import ABC, abstractmethod

from openlineage.client.event_v2 import Dataset


class BaseDataXLineagePlugin(ABC):
    """Abstract base class for DataX lineage plugins.

    Each plugin knows how to parse specific DataX reader/writer configurations
    into OpenLineage Dataset objects.
    """

    @abstractmethod
    def reader_names(self) -> list[str]:
        """Return the DataX reader plugin names this plugin handles (e.g. ['mysqlreader'])."""

    @abstractmethod
    def writer_names(self) -> list[str]:
        """Return the DataX writer plugin names this plugin handles (e.g. ['mysqlwriter'])."""

    @abstractmethod
    def parse_datasets(self, plugin_name: str, plugin_config: dict) -> list[Dataset]:
        """Parse a DataX reader/writer config into OpenLineage Dataset(s).

        Args:
            plugin_name: The DataX plugin name (e.g. 'mysqlreader').
            plugin_config: The 'parameter' dict from the DataX job config.

        Returns:
            A list of Dataset objects extracted from the config.
        """
