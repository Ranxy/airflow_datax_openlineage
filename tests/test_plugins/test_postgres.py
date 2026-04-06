from airflow_provider_datax_openlineage.lineage.plugins.postgres import (
    PostgresLineagePlugin,
    _build_schema_facet,
    _parse_jdbc_url,
)


class TestParseJdbcUrl:
    def test_standard_url(self):
        host, port, db = _parse_jdbc_url("jdbc:postgresql://10.0.0.2:5432/analytics")
        assert host == "10.0.0.2"
        assert port == 5432
        assert db == "analytics"

    def test_url_with_query_params(self):
        host, port, db = _parse_jdbc_url(
            "jdbc:postgresql://pghost:5433/warehouse?currentSchema=public"
        )
        assert host == "pghost"
        assert port == 5433
        assert db == "warehouse"

    def test_url_without_port(self):
        host, port, db = _parse_jdbc_url("jdbc:postgresql://pghost/mydb")
        assert host == "pghost"
        assert port == 5432  # default
        assert db == "mydb"

    def test_url_without_database(self):
        host, port, db = _parse_jdbc_url("jdbc:postgresql://pghost:5432/")
        assert host == "pghost"
        assert port == 5432
        assert db == ""


class TestPostgresLineagePlugin:
    def setup_method(self):
        self.plugin = PostgresLineagePlugin()

    def test_reader_writer_names(self):
        assert self.plugin.reader_names() == ["postgresqlreader"]
        assert self.plugin.writer_names() == ["postgresqlwriter"]

    def test_single_table(self):
        config = {
            "column": ["id", "name", "address"],
            "connection": [
                {
                    "jdbcUrl": ["jdbc:postgresql://10.0.0.2:5432/target_db"],
                    "table": ["customers"],
                }
            ]
        }
        datasets = self.plugin.parse_datasets("postgresqlwriter", config)
        assert len(datasets) == 1
        assert datasets[0].namespace == "postgres://10.0.0.2:5432"
        assert datasets[0].name == "target_db.customers"
        schema = datasets[0].facets["schema"]
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "address"

    def test_multiple_tables(self):
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:postgresql://10.0.0.2:5432/target_db"],
                    "table": ["customers", "orders"],
                }
            ]
        }
        datasets = self.plugin.parse_datasets("postgresqlwriter", config)
        assert len(datasets) == 2
        assert datasets[0].name == "target_db.customers"
        assert datasets[1].name == "target_db.orders"

    def test_multiple_connections(self):
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:postgresql://host1:5432/db1"],
                    "table": ["t1"],
                },
                {
                    "jdbcUrl": ["jdbc:postgresql://host2:5432/db2"],
                    "table": ["t2"],
                },
            ]
        }
        datasets = self.plugin.parse_datasets("postgresqlreader", config)
        assert len(datasets) == 2
        assert datasets[0].namespace == "postgres://host1:5432"
        assert datasets[1].namespace == "postgres://host2:5432"

    def test_jdbc_url_as_string(self):
        config = {
            "connection": [
                {
                    "jdbcUrl": "jdbc:postgresql://10.0.0.2:5432/mydb",
                    "table": ["t1"],
                }
            ]
        }
        datasets = self.plugin.parse_datasets("postgresqlreader", config)
        assert len(datasets) == 1
        assert datasets[0].namespace == "postgres://10.0.0.2:5432"

    def test_table_as_string(self):
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:postgresql://10.0.0.2:5432/mydb"],
                    "table": "single_table",
                }
            ]
        }
        datasets = self.plugin.parse_datasets("postgresqlreader", config)
        assert len(datasets) == 1
        assert datasets[0].name == "mydb.single_table"

    def test_empty_connection(self):
        datasets = self.plugin.parse_datasets("postgresqlreader", {"connection": []})
        assert datasets == []

    def test_missing_connection_key(self):
        datasets = self.plugin.parse_datasets("postgresqlreader", {})
        assert datasets == []

    def test_columns_attached_as_schema_facet(self):
        config = {
            "column": ["id", "name", "age"],
            "connection": [
                {
                    "jdbcUrl": ["jdbc:postgresql://10.0.0.2:5432/db"],
                    "table": ["t1", "t2"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("postgresqlreader", config)
        assert len(datasets) == 2
        for ds in datasets:
            assert "schema" in ds.facets
            assert [f.name for f in ds.facets["schema"].fields] == ["id", "name", "age"]

    def test_star_column_no_schema(self):
        config = {
            "column": ["*"],
            "connection": [
                {
                    "jdbcUrl": ["jdbc:postgresql://10.0.0.2:5432/db"],
                    "table": ["t1"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("postgresqlreader", config)
        assert len(datasets) == 1
        assert not datasets[0].facets

    def test_no_column_no_schema(self):
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:postgresql://10.0.0.2:5432/db"],
                    "table": ["t1"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("postgresqlreader", config)
        assert len(datasets) == 1
        assert not datasets[0].facets


class TestBuildSchemaFacet:
    def test_normal_columns(self):
        facet = _build_schema_facet(["id", "name", "email"])
        assert facet is not None
        assert len(facet.fields) == 3

    def test_star_returns_none(self):
        assert _build_schema_facet(["*"]) is None

    def test_empty_returns_none(self):
        assert _build_schema_facet([]) is None
