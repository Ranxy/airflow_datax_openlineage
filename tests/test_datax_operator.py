from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow_provider_datax_openlineage.operators.datax import DataXOperator


@pytest.fixture()
def datax_job_config(tmp_path):
    """Write a sample DataX job config to a temp file and return its path."""
    config = {
        "job": {
            "content": [
                {
                    "reader": {
                        "name": "mysqlreader",
                        "parameter": {
                            "column": ["id", "name", "email"],
                            "connection": [
                                {
                                    "jdbcUrl": ["jdbc:mysql://10.0.0.1:3306/src_db"],
                                    "table": ["users"],
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
                                        "jdbc:postgresql://10.0.0.2:5432/dst_db"
                                    ],
                                    "table": ["users_copy"],
                                }
                            ]
                        },
                    },
                }
            ]
        }
    }
    config_file = tmp_path / "job.json"
    config_file.write_text(json.dumps(config))
    return str(config_file)


@pytest.fixture()
def make_operator(datax_job_config):
    """Factory to create a DataXOperator with sensible defaults."""

    def _make(**kwargs):
        defaults = {
            "task_id": "test_datax",
            "job_config": datax_job_config,
            "datax_home": "/opt/datax",
            "openlineage_url": "http://localhost:5000",
        }
        defaults.update(kwargs)
        return DataXOperator(**defaults)

    return _make


class TestDataXOperatorInit:
    def test_basic_init(self, make_operator):
        op = make_operator()
        assert op.datax_home == "/opt/datax"
        assert op.datax_python == "python"
        assert op.openlineage_url == "http://localhost:5000"
        assert op.openlineage_endpoint == "api/v1/lineage"
        assert op.openlineage_namespace == "datax"

    def test_custom_endpoint(self, make_operator):
        op = make_operator(openlineage_endpoint="custom/lineage")
        assert op.openlineage_endpoint == "custom/lineage"

    def test_empty_endpoint(self, make_operator):
        op = make_operator(
            openlineage_url="http://localhost:5000/api/v1/lineage",
            openlineage_endpoint="",
        )
        assert op.openlineage_endpoint == ""

    def test_datax_home_from_env(self, datax_job_config):
        with patch.dict(os.environ, {"DATAX_HOME": "/env/datax"}):
            op = DataXOperator(
                task_id="t", job_config=datax_job_config, openlineage_url="http://x"
            )
        assert op.datax_home == "/env/datax"

    def test_openlineage_url_from_env(self, datax_job_config):
        with patch.dict(os.environ, {"OPENLINEAGE_URL": "http://env-ol:5000"}):
            op = DataXOperator(
                task_id="t", job_config=datax_job_config, datax_home="/opt/datax"
            )
        assert op.openlineage_url == "http://env-ol:5000"

    def test_param_overrides_env(self, datax_job_config):
        with patch.dict(
            os.environ,
            {"DATAX_HOME": "/env/datax", "OPENLINEAGE_URL": "http://env:5000"},
        ):
            op = DataXOperator(
                task_id="t",
                job_config=datax_job_config,
                datax_home="/param/datax",
                openlineage_url="http://param:5000",
            )
        assert op.datax_home == "/param/datax"
        assert op.openlineage_url == "http://param:5000"


class TestGetOpenLineageClient:
    def test_endpoint_passed_to_http_config(self, make_operator):
        with patch(
            "airflow_provider_datax_openlineage.operators.datax.HttpConfig"
        ) as mock_config, patch(
            "airflow_provider_datax_openlineage.operators.datax.HttpTransport"
        ):
            op = make_operator(openlineage_endpoint="custom/endpoint")
            op._get_openlineage_client()
            mock_config.assert_called_once_with(
                url="http://localhost:5000", endpoint="custom/endpoint"
            )

    def test_empty_endpoint_for_full_url(self, make_operator):
        with patch(
            "airflow_provider_datax_openlineage.operators.datax.HttpConfig"
        ) as mock_config, patch(
            "airflow_provider_datax_openlineage.operators.datax.HttpTransport"
        ):
            op = make_operator(
                openlineage_url="http://localhost:5000/api/v1/lineage",
                openlineage_endpoint="",
            )
            op._get_openlineage_client()
            mock_config.assert_called_once_with(
                url="http://localhost:5000/api/v1/lineage", endpoint=""
            )


class TestBuildCommand:
    def test_basic_command(self, make_operator):
        op = make_operator()
        cmd = op._build_command()
        assert cmd[0] == "python"
        assert cmd[1] == "/opt/datax/bin/datax.py"
        assert cmd[-1] == op.job_config

    def test_command_with_jvm_params(self, make_operator):
        op = make_operator(jvm_params={"key1": "val1", "key2": "val2"})
        cmd = op._build_command()
        assert "-p" in cmd
        p_index = cmd.index("-p")
        params_str = cmd[p_index + 1]
        assert "-Dkey1=val1" in params_str
        assert "-Dkey2=val2" in params_str

    def test_command_with_custom_python(self, make_operator):
        op = make_operator(datax_python="/usr/bin/python3")
        cmd = op._build_command()
        assert cmd[0] == "/usr/bin/python3"


class TestExecute:
    def _make_context(self):
        dag = MagicMock()
        dag.dag_id = "test_dag"
        task = MagicMock()
        task.task_id = "test_task"
        return {"dag": dag, "task": task}

    @patch("airflow_provider_datax_openlineage.operators.datax.subprocess.Popen")
    @patch("airflow_provider_datax_openlineage.operators.datax.OpenLineageClient")
    @patch(
        "airflow_provider_datax_openlineage.operators.datax.HttpTransport",
    )
    def test_success_emits_start_and_complete(
        self, mock_transport_cls, mock_ol_cls, mock_popen, make_operator
    ):
        # Setup subprocess mock
        process = MagicMock()
        process.stdout = iter(["line1\n", "line2\n"])
        process.returncode = 0
        process.wait.return_value = 0
        mock_popen.return_value = process

        # Setup OL client mock
        mock_client = MagicMock()
        mock_ol_cls.return_value = mock_client

        op = make_operator()
        context = self._make_context()
        result = op.execute(context)

        assert result["returncode"] == 0
        assert mock_client.emit.call_count == 2

        # First call should be START
        start_event = mock_client.emit.call_args_list[0][0][0]
        assert start_event.eventType.value == "START"
        assert len(start_event.inputs) == 1
        assert len(start_event.outputs) == 1

        # Second call should be COMPLETE
        complete_event = mock_client.emit.call_args_list[1][0][0]
        assert complete_event.eventType.value == "COMPLETE"

        # Same run ID
        assert start_event.run.runId == complete_event.run.runId

    @patch("airflow_provider_datax_openlineage.operators.datax.subprocess.Popen")
    @patch("airflow_provider_datax_openlineage.operators.datax.OpenLineageClient")
    @patch(
        "airflow_provider_datax_openlineage.operators.datax.HttpTransport",
    )
    def test_failure_emits_start_and_fail(
        self, mock_transport_cls, mock_ol_cls, mock_popen, make_operator
    ):
        process = MagicMock()
        process.stdout = iter(["error output\n"])
        process.returncode = 1
        process.wait.return_value = 1
        mock_popen.return_value = process

        mock_client = MagicMock()
        mock_ol_cls.return_value = mock_client

        op = make_operator()
        context = self._make_context()

        with pytest.raises(AirflowException, match="exit code 1"):
            op.execute(context)

        assert mock_client.emit.call_count == 2

        start_event = mock_client.emit.call_args_list[0][0][0]
        assert start_event.eventType.value == "START"

        fail_event = mock_client.emit.call_args_list[1][0][0]
        assert fail_event.eventType.value == "FAIL"

    def test_missing_datax_home_raises(self, datax_job_config):
        with patch.dict(os.environ, {}, clear=True):
            op = DataXOperator(
                task_id="t",
                job_config=datax_job_config,
                openlineage_url="http://localhost:5000",
            )
            # Ensure datax_home is not set from env either
            op.datax_home = None

        with pytest.raises(AirflowException, match="datax_home is not set"):
            op.execute(self._make_context())

    @patch("airflow_provider_datax_openlineage.operators.datax.subprocess.Popen")
    def test_no_openlineage_url_skips_emit(self, mock_popen, datax_job_config):
        """When no OpenLineage URL is configured, DataX still runs but no events emitted."""
        process = MagicMock()
        process.stdout = iter(["ok\n"])
        process.returncode = 0
        process.wait.return_value = 0
        mock_popen.return_value = process

        op = DataXOperator(
            task_id="t",
            job_config=datax_job_config,
            datax_home="/opt/datax",
        )
        op.openlineage_url = None

        result = op.execute(self._make_context())
        assert result["returncode"] == 0

    @patch("airflow_provider_datax_openlineage.operators.datax.subprocess.Popen")
    @patch("airflow_provider_datax_openlineage.operators.datax.OpenLineageClient")
    @patch("airflow_provider_datax_openlineage.operators.datax.HttpTransport")
    def test_job_name_from_context(
        self, mock_transport_cls, mock_ol_cls, mock_popen, make_operator
    ):
        process = MagicMock()
        process.stdout = iter([])
        process.returncode = 0
        process.wait.return_value = 0
        mock_popen.return_value = process

        mock_client = MagicMock()
        mock_ol_cls.return_value = mock_client

        op = make_operator()
        context = self._make_context()
        op.execute(context)

        start_event = mock_client.emit.call_args_list[0][0][0]
        assert start_event.job.name == "test_dag.test_task"
        assert start_event.job.namespace == "datax"


class TestOnKill:
    def test_on_kill_terminates_process(self, make_operator):
        op = make_operator()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None  # still running
        op._process = mock_proc

        op.on_kill()

        mock_proc.terminate.assert_called_once()

    def test_on_kill_no_op_when_no_process(self, make_operator):
        op = make_operator()
        op._process = None
        op.on_kill()  # should not raise

    def test_on_kill_no_op_when_already_finished(self, make_operator):
        op = make_operator()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0  # already finished
        op._process = mock_proc

        op.on_kill()

        mock_proc.terminate.assert_not_called()
