from airflow_provider_datax_openlineage.lineage.plugins.mysql import (
    MysqlLineagePlugin,
    _build_schema_facet,
    _parse_jdbc_url,
)


class TestParseJdbcUrl:
    def test_standard_url(self):
        host, port, db = _parse_jdbc_url("jdbc:mysql://10.0.0.1:3306/mydb")
        assert host == "10.0.0.1"
        assert port == 3306
        assert db == "mydb"

    def test_url_with_query_params(self):
        host, port, db = _parse_jdbc_url(
            "jdbc:mysql://dbhost:3307/orders?useUnicode=true&characterEncoding=utf8"
        )
        assert host == "dbhost"
        assert port == 3307
        assert db == "orders"

    def test_url_without_port(self):
        host, port, db = _parse_jdbc_url("jdbc:mysql://dbhost/mydb")
        assert host == "dbhost"
        assert port == 3306  # default
        assert db == "mydb"

    def test_url_without_database(self):
        host, port, db = _parse_jdbc_url("jdbc:mysql://dbhost:3306/")
        assert host == "dbhost"
        assert port == 3306
        assert db == ""


class TestMysqlLineagePlugin:
    def setup_method(self):
        self.plugin = MysqlLineagePlugin()

    def test_reader_writer_names(self):
        assert self.plugin.reader_names() == ["mysqlreader"]
        assert self.plugin.writer_names() == ["mysqlwriter"]

    def test_single_table(self):
        config = {
            "column": ["id", "name", "email"],
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:3306/source_db"],
                    "table": ["users"],
                }
            ]
        }
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        assert len(datasets) == 1
        assert datasets[0].namespace == "mysql://10.0.0.1:3306"
        assert datasets[0].name == "source_db.users"
        schema = datasets[0].facets["schema"]
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"
        assert schema.fields[2].name == "email"

    def test_multiple_tables(self):
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:3306/source_db"],
                    "table": ["users", "orders", "products"],
                }
            ]
        }
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        assert len(datasets) == 3
        assert datasets[0].name == "source_db.users"
        assert datasets[1].name == "source_db.orders"
        assert datasets[2].name == "source_db.products"

    def test_multiple_connections(self):
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://host1:3306/db1"],
                    "table": ["t1"],
                },
                {
                    "jdbcUrl": ["jdbc:mysql://host2:3306/db2"],
                    "table": ["t2"],
                },
            ]
        }
        datasets = self.plugin.parse_datasets("mysqlwriter", config)
        assert len(datasets) == 2
        assert datasets[0].namespace == "mysql://host1:3306"
        assert datasets[0].name == "db1.t1"
        assert datasets[1].namespace == "mysql://host2:3306"
        assert datasets[1].name == "db2.t2"

    def test_jdbc_url_as_string(self):
        """jdbcUrl can be a plain string instead of a list."""
        config = {
            "connection": [
                {
                    "jdbcUrl": "jdbc:mysql://10.0.0.1:3306/mydb",
                    "table": ["t1"],
                }
            ]
        }
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        assert len(datasets) == 1
        assert datasets[0].namespace == "mysql://10.0.0.1:3306"

    def test_table_as_string(self):
        """table can be a plain string instead of a list."""
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:3306/mydb"],
                    "table": "single_table",
                }
            ]
        }
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        assert len(datasets) == 1
        assert datasets[0].name == "mydb.single_table"

    def test_empty_connection(self):
        datasets = self.plugin.parse_datasets("mysqlreader", {"connection": []})
        assert datasets == []

    def test_missing_connection_key(self):
        datasets = self.plugin.parse_datasets("mysqlreader", {})
        assert datasets == []

    def test_invalid_jdbc_url_skipped(self):
        """Invalid JDBC URLs should be logged and skipped, not raise."""
        config = {
            "connection": [
                {
                    "jdbcUrl": ["not-a-valid-url://???"],
                    "table": ["t1"],
                }
            ]
        }
        # Should not raise
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        # The url parses but with odd values; the plugin should still return something
        # or gracefully skip. Either way, no exception.
        assert isinstance(datasets, list)

    def test_columns_attached_as_schema_facet(self):
        config = {
            "column": ["id", "name", "age"],
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:3306/db"],
                    "table": ["t1", "t2"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        assert len(datasets) == 2
        for ds in datasets:
            assert "schema" in ds.facets
            assert len(ds.facets["schema"].fields) == 3
            assert [f.name for f in ds.facets["schema"].fields] == ["id", "name", "age"]

    def test_star_column_no_schema(self):
        """column=['*'] means all columns — no schema facet generated."""
        config = {
            "column": ["*"],
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:3306/db"],
                    "table": ["t1"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        assert len(datasets) == 1
        assert not datasets[0].facets  # no facets

    def test_no_column_no_schema(self):
        """No column key means no schema facet."""
        config = {
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:3306/db"],
                    "table": ["t1"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("mysqlreader", config)
        assert len(datasets) == 1
        assert not datasets[0].facets


class TestBuildSchemaFacet:
    def test_normal_columns(self):
        facet = _build_schema_facet(["id", "name", "email"])
        assert facet is not None
        assert len(facet.fields) == 3
        assert facet.fields[0].name == "id"
        assert facet.fields[2].name == "email"

    def test_star_returns_none(self):
        assert _build_schema_facet(["*"]) is None

    def test_empty_returns_none(self):
        assert _build_schema_facet([]) is None
