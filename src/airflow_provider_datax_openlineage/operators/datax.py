from __future__ import annotations

import importlib
import json
import logging
import os
import subprocess
import uuid
from datetime import datetime, timezone
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.transport.http import HttpConfig, HttpTransport

from airflow_provider_datax_openlineage.lineage.parser import LineageParser
from airflow_provider_datax_openlineage.lineage.plugins.base import BaseDataXLineagePlugin

log = logging.getLogger(__name__)

_PRODUCER = "https://github.com/airflow-provider-datax-openlineage"


class DataXOperator(BaseOperator):
    """Airflow operator that executes a DataX job and emits OpenLineage lineage events.

    :param job_config: Path to the DataX job JSON configuration file.
    :param datax_home: DataX installation directory. Falls back to ``DATAX_HOME`` env var.
    :param datax_python: Python executable used to run ``datax.py``. Defaults to ``"python"``.
    :param jvm_params: Optional dict of JVM parameters passed via ``-p`` to DataX.
    :param openlineage_url: OpenLineage API base URL (e.g. ``"http://host:port"``).
        Falls back to ``OPENLINEAGE_URL`` env var. If neither is set, lineage emission is skipped.
    :param openlineage_endpoint: API endpoint path appended to ``openlineage_url``.
        Defaults to ``"api/v1/lineage"``. Set to ``""`` if ``openlineage_url`` already contains the full path.
    :param openlineage_namespace: Namespace used in OpenLineage Job. Defaults to ``"datax"``.
    :param extra_lineage_plugins: Additional lineage plugins for custom DataX reader/writer types.
    """

    template_fields = ("job_config", "jvm_params")
    ui_color = "#4fc3f7"

    def __init__(
        self,
        *,
        job_config: str,
        datax_home: str | None = None,
        datax_python: str = "python",
        jvm_params: dict[str, str] | None = None,
        openlineage_url: str | None = None,
        openlineage_endpoint: str = "api/v1/lineage",
        openlineage_namespace: str = "datax",
        extra_lineage_plugins: list[BaseDataXLineagePlugin] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.job_config = job_config
        self.datax_home = datax_home or os.environ.get("DATAX_HOME")
        self.datax_python = datax_python
        self.jvm_params = jvm_params
        self.openlineage_url = openlineage_url or os.environ.get("OPENLINEAGE_URL")
        self.openlineage_endpoint = openlineage_endpoint
        self.openlineage_namespace = openlineage_namespace
        self.extra_lineage_plugins = extra_lineage_plugins
        self._process: subprocess.Popen | None = None  # type: ignore[type-arg]

    def execute(self, context: Any) -> dict[str, Any]:
        if not self.datax_home:
            raise AirflowException(
                "datax_home is not set. Provide it as a parameter or set the DATAX_HOME environment variable."
            )

        emit_manually = self._should_emit_manual_lineage()
        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []
        run_id: str | None = None
        job_name: str | None = None
        ol_client: OpenLineageClient | None = None

        if emit_manually:
            inputs, outputs = self._extract_lineage_datasets()
            run_id = str(uuid.uuid4())
            job_name = self._build_job_name(context)
            ol_client = self._get_openlineage_client()

            self._emit_event(ol_client, RunState.START, run_id, job_name, inputs, outputs)

        try:
            result = self._run_datax()
        except Exception:
            if emit_manually and run_id and job_name:
                self._emit_event(ol_client, RunState.FAIL, run_id, job_name, inputs, outputs)
            raise

        if emit_manually and run_id and job_name:
            self._emit_event(ol_client, RunState.COMPLETE, run_id, job_name, inputs, outputs)

        return result

    def get_openlineage_facets_on_start(self) -> Any:
        operator_lineage_cls, dataset_cls = self._get_openlineage_provider_types()
        if operator_lineage_cls is None or dataset_cls is None:
            return None

        try:
            inputs, outputs = self._extract_lineage_datasets()
        except Exception:
            log.exception("Failed to build OpenLineage metadata for DataXOperator")
            return None

        return operator_lineage_cls(
            inputs=[
                dataset_cls(namespace=dataset.namespace, name=dataset.name, facets=dataset.facets)
                for dataset in inputs
            ],
            outputs=[
                dataset_cls(namespace=dataset.namespace, name=dataset.name, facets=dataset.facets)
                for dataset in outputs
            ],
        )

    def on_kill(self) -> None:
        if self._process and self._process.poll() is None:
            log.info("Killing DataX process (pid=%s)", self._process.pid)
            self._process.terminate()

    # ---- private helpers ----

    def _extract_lineage_datasets(
        self,
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        job_config_dict = self._read_job_config()
        parser = LineageParser(extra_plugins=self.extra_lineage_plugins)
        return parser.parse(job_config_dict)

    def _build_job_name(self, context: Any) -> str:
        dag = context.get("dag")
        task = context.get("task")
        return f"{dag.dag_id}.{task.task_id}" if dag and task else self.task_id

    def _should_emit_manual_lineage(self) -> bool:
        if not self.openlineage_url:
            return False
        return not self._is_official_openlineage_provider_available()

    def _is_official_openlineage_provider_available(self) -> bool:
        try:
            conf_module = importlib.import_module("airflow.providers.openlineage.conf")
            listener_module = importlib.import_module(
                "airflow.providers.openlineage.plugins.listener"
            )
        except (ImportError, AttributeError):
            log.debug("Official Airflow OpenLineage provider is not importable.")
            return False

        is_disabled = getattr(conf_module, "is_disabled", None)
        get_listener = getattr(listener_module, "get_openlineage_listener", None)
        if not callable(is_disabled) or not callable(get_listener):
            log.debug("Official Airflow OpenLineage provider is missing required APIs.")
            return False

        try:
            if is_disabled():
                log.debug("Official Airflow OpenLineage provider is disabled or unconfigured.")
                return False
            if not get_listener():
                log.debug("Official Airflow OpenLineage listener is unavailable.")
                return False
        except Exception:
            log.exception("Failed to determine whether official OpenLineage integration is active")
            return False

        return True

    def _get_openlineage_provider_types(self) -> tuple[type[Any] | None, type[Any] | None]:
        try:
            facet_module = importlib.import_module(
                "airflow.providers.common.compat.openlineage.facet"
            )
            extractors_module = importlib.import_module(
                "airflow.providers.openlineage.extractors"
            )
        except ImportError:
            log.debug("Official Airflow OpenLineage provider types are unavailable.")
            return None, None

        dataset_cls = getattr(facet_module, "Dataset", None)
        operator_lineage_cls = getattr(extractors_module, "OperatorLineage", None)
        if dataset_cls is None or operator_lineage_cls is None:
            log.debug("Official Airflow OpenLineage provider types are incomplete.")
            return None, None

        return operator_lineage_cls, dataset_cls

    def _read_job_config(self) -> dict:
        log.info("Reading DataX job config: %s", self.job_config)
        with open(self.job_config) as f:
            return json.load(f)

    def _build_command(self) -> list[str]:
        cmd = [self.datax_python, os.path.join(self.datax_home, "bin", "datax.py")]  # type: ignore[arg-type]
        if self.jvm_params:
            params_str = " ".join(f"-D{k}={v}" for k, v in self.jvm_params.items())
            cmd.extend(["-p", params_str])
        cmd.append(self.job_config)
        return cmd

    def _run_datax(self) -> dict[str, Any]:
        cmd = self._build_command()
        log.info("Executing DataX command: %s", " ".join(cmd))

        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Stream stdout to the Airflow task log
        stdout_lines: list[str] = []
        for line in self._process.stdout:  # type: ignore[union-attr]
            line = line.rstrip("\n")
            log.info(line)
            stdout_lines.append(line)

        self._process.wait()

        if self._process.returncode != 0:
            raise AirflowException(
                f"DataX job failed with exit code {self._process.returncode}"
            )

        return {"returncode": self._process.returncode, "stdout": "\n".join(stdout_lines)}

    def _get_openlineage_client(self) -> OpenLineageClient | None:
        if not self.openlineage_url:
            log.info("OpenLineage URL not configured, lineage emission is disabled.")
            return None

        transport = HttpTransport(
            HttpConfig(url=self.openlineage_url, endpoint=self.openlineage_endpoint)
        )
        return OpenLineageClient(transport=transport)

    def _emit_event(
        self,
        client: OpenLineageClient | None,
        event_type: RunState,
        run_id: str,
        job_name: str,
        inputs: list[InputDataset],
        outputs: list[OutputDataset],
    ) -> None:
        if client is None:
            return

        event = RunEvent(
            eventTime=datetime.now(timezone.utc).isoformat(),
            producer=_PRODUCER,
            run=Run(runId=run_id),
            job=Job(namespace=self.openlineage_namespace, name=job_name),
            eventType=event_type,
            inputs=inputs,
            outputs=outputs, 
        )

        log.info("Emitting OpenLineage %s event for run %s", event_type.value, run_id)
        try:
            client.emit(event)
        except Exception:
            log.exception("Failed to emit OpenLineage event")
