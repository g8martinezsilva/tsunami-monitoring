"""Check marker recovery using the task graph with Airflow dependencies mocked."""

import importlib.util
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch


class Task:
    def __init__(self, function, args=()):
        self.function = function
        self.args = args
        self.upstream = [arg for arg in args if isinstance(arg, Task)]

    def __rshift__(self, downstream):
        downstream.upstream.append(self)
        return downstream

    def run(self, results):
        if self not in results:
            for task in self.upstream:
                task.run(results)
            args = [results[arg] if isinstance(arg, Task) else arg for arg in self.args]
            results[self] = self.function(*args)
        return results[self]


class SilverDAGTests(unittest.TestCase):
    def test_failed_matching_leaves_marker_and_retry_reuses_confirmed_load(self):
        tasks = {}

        def task(function):
            def bind(*args):
                bound = Task(function, args)
                tasks[function.__name__] = bound
                return bound
            return bind

        airflow_modules = {
            "airflow.decorators": SimpleNamespace(task=task, dag=lambda **kwargs: lambda fn: fn),
            "airflow.providers.postgres.hooks.postgres": SimpleNamespace(PostgresHook=Mock()),
            "airflow.sensors.filesystem": SimpleNamespace(FileSensor=lambda **kwargs: Task(lambda: True)),
        }
        path = Path(__file__).resolve().parents[1] / "airflow/dags/silver_processing_dag.py"
        spec = importlib.util.spec_from_file_location("silver_dag_under_test", path)
        module = importlib.util.module_from_spec(spec)

        with TemporaryDirectory() as directory, patch.dict(os.environ, {"RAW_DIR": directory}):
            marker = Path(directory) / "_new" / "bronze_manifest_test.json"
            marker.parent.mkdir()
            marker.touch()
            with patch.dict(sys.modules, airflow_modules):
                spec.loader.exec_module(module)

            # Supply the logical date as Airflow does. PostgreSQL effects are mocked in this test.
            tasks["bronze_to_silver"].args += (datetime(2026, 11, 19, tzinfo=timezone.utc),)
            results = {}
            with (patch.object(module, "process_bronze_run", return_value={
                      "bronze_manifest": marker.name, "outputs": {}
                  }) as preprocess,
                  patch.object(module, "load_silver_run", return_value={"accepted": 1}) as load,
                  patch.object(module, "match_events", side_effect=[RuntimeError("matching failed"), {}]) as match):
                with self.assertRaisesRegex(RuntimeError, "matching failed"):
                    tasks["clear_markers"].run(results)
                self.assertTrue(marker.exists())
                self.assertIn(tasks["load_to_database"], results)

                tasks["clear_markers"].run(results)
                self.assertFalse(marker.exists())
                preprocess.assert_called_once()
                load.assert_called_once()
                self.assertEqual(match.call_count, 2)
