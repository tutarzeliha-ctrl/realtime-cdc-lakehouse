"""DAG structure checks. Skipped when Airflow is not installed (e.g. in the consumer's venv)."""

from pathlib import Path
import sys

import pytest

pytest.importorskip("airflow")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(ROOT / "dags"))


def test_dags_load_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"cdc_freshness_check", "daily_analytics"}


def test_daily_analytics_runs_dbt_only_after_freshness_gate(dagbag):
    dag = dagbag.dags["daily_analytics"]
    assert dag.get_task("dbt_build").upstream_task_ids == {"freshness_gate"}
    assert dag.get_task("reconcile_source_with_snapshot").upstream_task_ids == {"dbt_build"}


def test_freshness_check_runs_every_five_minutes(dagbag):
    dag = dagbag.dags["cdc_freshness_check"]
    assert dag.timetable.expression == "*/5 * * * *"
