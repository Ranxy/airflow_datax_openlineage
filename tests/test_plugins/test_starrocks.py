from airflow_provider_datax_openlineage.lineage.plugins.starrocks import (
    StarrocksLineagePlugin,
    _build_schema_facet,
    _parse_jdbc_url,
)


class TestParseJdbcUrl:
    def test_standard_url(self):
        host, port, db = _parse_jdbc_url("jdbc:mysql://10.0.0.1:9030/mydb")
        assert host == "10.0.0.1"
        assert port == 9030
        assert db == "mydb"

    def test_url_with_query_params(self):
        host, port, db = _parse_jdbc_url(
            "jdbc:mysql://dbhost:9031/warehouse?useUnicode=true&characterEncoding=utf8"
        )
        assert host == "dbhost"
        assert port == 9031
        assert db == "warehouse"

    def test_url_without_port(self):
        host, port, db = _parse_jdbc_url("jdbc:mysql://dbhost/mydb")
        assert host == "dbhost"
        assert port == 9030  # default
        assert db == "mydb"

    def test_url_without_database(self):
        host, port, db = _parse_jdbc_url("jdbc:mysql://dbhost:9030/")
        assert host == "dbhost"
        assert port == 9030
        assert db == ""


class TestBuildSchemaFacet:
    def test_returns_none_for_star(self):
        assert _build_schema_facet(["*"]) is None
        assert _build_schema_facet([]) is None

    def test_builds_fields(self):
        facet = _build_schema_facet(["id", "name"])
        assert [f.name for f in facet.fields] == ["id", "name"]


class TestStarrocksLineagePlugin:
    def setup_method(self):
        self.plugin = StarrocksLineagePlugin()

    def test_reader_writer_names(self):
        assert self.plugin.reader_names() == ["starrocksreader"]
        assert self.plugin.writer_names() == ["starrockswriter"]

    def test_writer_config_with_string_url(self):
        # Mirrors the starrockswriter job template: a single jdbcUrl string plus
        # selectedDatabase, which is how the e2e fixture configures it.
        config = {
            "column": ["order_id", "net_amount"],
            "connection": [
                {
                    "jdbcUrl": "jdbc:mysql://127.0.0.1:9030/e2e_ods",
                    "selectedDatabase": "e2e_ods",
                    "table": ["dwd_order_fact"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("starrockswriter", config)
        assert len(datasets) == 1
        assert datasets[0].namespace == "starrocks://127.0.0.1:9030"
        assert datasets[0].name == "e2e_ods.dwd_order_fact"
        assert [f.name for f in datasets[0].facets["schema"].fields] == ["order_id", "net_amount"]

    def test_selected_database_is_the_fallback(self):
        config = {
            "column": ["id"],
            "connection": [
                {
                    "jdbcUrl": "jdbc:mysql://dbhost:9030",
                    "selectedDatabase": "warehouse",
                    "table": ["orders"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("starrockswriter", config)
        assert datasets[0].name == "warehouse.orders"

    def test_reader_config_with_url_list(self):
        config = {
            "column": ["id", "name"],
            "connection": [
                {
                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:9030/source_db"],
                    "table": ["users", "orders"],
                }
            ],
        }
        datasets = self.plugin.parse_datasets("starrocksreader", config)
        assert [d.name for d in datasets] == ["source_db.users", "source_db.orders"]

    def test_missing_jdbc_url_yields_no_datasets(self):
        datasets = self.plugin.parse_datasets("starrockswriter", {"column": ["id"], "connection": []})
        assert datasets == []
