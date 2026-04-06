import logging

import pytest
from openlineage.client.event_v2 import Dataset, InputDataset, OutputDataset

from airflow_provider_datax_openlineage.lineage.parser import LineageParser
from airflow_provider_datax_openlineage.lineage.plugins.base import BaseDataXLineagePlugin


@pytest.fixture()
def mysql_to_postgres_config():
    return {
        "job": {
            "setting": {"speed": {"channel": 1}},
            "content": [
                {
                    "reader": {
                        "name": "mysqlreader",
                        "parameter": {
                            "column": ["id", "name", "email"],
                            "connection": [
                                {
                                    "jdbcUrl": [
                                        "jdbc:mysql://10.0.0.1:3306/source_db"
                                    ],
                                    "table": ["users", "orders"],
                                }
                            ]
                        },
                    },
                    "writer": {
                        "name": "postgresqlwriter",
                        "parameter": {
                            "column": ["id", "name", "email"],
                            "connection": [
                                {
                                    "jdbcUrl": [
                                        "jdbc:postgresql://10.0.0.2:5432/target_db"
                                    ],
                                    "table": ["users_copy"],
                                }
                            ]
                        },
                    },
                }
            ],
        }
    }


@pytest.fixture()
def multi_content_config():
    return {
        "job": {
            "content": [
                {
                    "reader": {
                        "name": "mysqlreader",
                        "parameter": {
                            "connection": [
                                {
                                    "jdbcUrl": ["jdbc:mysql://host1:3306/db1"],
                                    "table": ["t1"],
                                }
                            ]
                        },
                    },
                    "writer": {
                        "name": "postgresqlwriter",
                        "parameter": {
                            "connection": [
                                {
                                    "jdbcUrl": [
                                        "jdbc:postgresql://host2:5432/db2"
                                    ],
                                    "table": ["t2"],
                                }
                            ]
                        },
                    },
                },
                {
                    "reader": {
                        "name": "mysqlreader",
                        "parameter": {
                            "connection": [
                                {
                                    "jdbcUrl": ["jdbc:mysql://host3:3306/db3"],
                                    "table": ["t3"],
                                }
                            ]
                        },
                    },
                    "writer": {
                        "name": "mysqlwriter",
                        "parameter": {
                            "connection": [
                                {
                                    "jdbcUrl": ["jdbc:mysql://host4:3306/db4"],
                                    "table": ["t4"],
                                }
                            ]
                        },
                    },
                },
            ]
        }
    }


class TestLineageParser:
    def test_parse_mysql_to_postgres(self, mysql_to_postgres_config):
        parser = LineageParser()
        inputs, outputs = parser.parse(mysql_to_postgres_config)

        assert len(inputs) == 2
        assert all(isinstance(i, InputDataset) for i in inputs)
        assert inputs[0].namespace == "mysql://10.0.0.1:3306"
        assert inputs[0].name == "source_db.users"
        assert inputs[1].name == "source_db.orders"

        # Verify column-level schema facets
        for inp in inputs:
            assert "schema" in inp.facets
            assert [f.name for f in inp.facets["schema"].fields] == ["id", "name", "email"]

        assert len(outputs) == 1
        assert all(isinstance(o, OutputDataset) for o in outputs)
        assert outputs[0].namespace == "postgres://10.0.0.2:5432"
        assert outputs[0].name == "target_db.users_copy"
        assert "schema" in outputs[0].facets
        assert [f.name for f in outputs[0].facets["schema"].fields] == ["id", "name", "email"]

    def test_parse_multi_content(self, multi_content_config):
        parser = LineageParser()
        inputs, outputs = parser.parse(multi_content_config)

        assert len(inputs) == 2
        assert inputs[0].namespace == "mysql://host1:3306"
        assert inputs[0].name == "db1.t1"
        assert inputs[1].namespace == "mysql://host3:3306"
        assert inputs[1].name == "db3.t3"

        assert len(outputs) == 2
        assert outputs[0].namespace == "postgres://host2:5432"
        assert outputs[0].name == "db2.t2"
        assert outputs[1].namespace == "mysql://host4:3306"
        assert outputs[1].name == "db4.t4"

    def test_unknown_reader_writer_warns(self, caplog):
        config = {
            "job": {
                "content": [
                    {
                        "reader": {
                            "name": "hdfsreader",
                            "parameter": {"path": "/data/input"},
                        },
                        "writer": {
                            "name": "hdfswriter",
                            "parameter": {"path": "/data/output"},
                        },
                    }
                ]
            }
        }
        parser = LineageParser()
        with caplog.at_level(logging.WARNING):
            inputs, outputs = parser.parse(config)

        assert inputs == []
        assert outputs == []
        assert "No lineage plugin registered for DataX reader: hdfsreader" in caplog.text
        assert "No lineage plugin registered for DataX writer: hdfswriter" in caplog.text

    def test_empty_config(self):
        parser = LineageParser()
        inputs, outputs = parser.parse({})
        assert inputs == []
        assert outputs == []

    def test_empty_content(self):
        parser = LineageParser()
        inputs, outputs = parser.parse({"job": {"content": []}})
        assert inputs == []
        assert outputs == []

    def test_extra_plugin(self):
        """User-supplied extra plugins are registered and used."""

        class FakePlugin(BaseDataXLineagePlugin):
            def reader_names(self):
                return ["fakereader"]

            def writer_names(self):
                return ["fakewriter"]

            def parse_datasets(self, plugin_name, plugin_config):
                return [Dataset(namespace="fake://host", name="fake.table")]

        config = {
            "job": {
                "content": [
                    {
                        "reader": {
                            "name": "fakereader",
                            "parameter": {},
                        },
                        "writer": {
                            "name": "fakewriter",
                            "parameter": {},
                        },
                    }
                ]
            }
        }
        parser = LineageParser(extra_plugins=[FakePlugin()])
        inputs, outputs = parser.parse(config)

        assert len(inputs) == 1
        assert inputs[0].namespace == "fake://host"
        assert inputs[0].name == "fake.table"
        assert len(outputs) == 1
