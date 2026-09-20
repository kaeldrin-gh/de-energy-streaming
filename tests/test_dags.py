"""DAG integrity tests: every DAG must import and stay wired to its jobs.

Runs in CI; skipped locally unless Airflow is installed. The Docker image
(``docker/airflow/requirements.txt``) pins the provider versions this test
targets.
"""

from __future__ import annotations

import importlib.metadata
from pathlib import Path

import pytest

try:
    importlib.metadata.version("apache-airflow")
except importlib.metadata.PackageNotFoundError:
    # The repository's own airflow/ directory is importable as a namespace
    # package, so check the distribution instead of pytest.importorskip.
    pytest.skip("apache-airflow not installed", allow_module_level=True)

from airflow.models import DAG, DagBag  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
JOBS_DIR = REPO_ROOT / "spark" / "jobs"

EXPECTED_SCHEDULES = {
    "energy_batch_pipeline": "0 * * * *",
    "energy_history_backfill": "0 3 * * *",
    "energy_healthcheck": "*/30 * * * *",
    "energy_news_ingest": "0 6 * * *",
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    bag = DagBag(str(DAGS_DIR), include_examples=False)
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    return bag


def get_dag(dagbag: DagBag, dag_id: str) -> DAG:
    """DagBag.get_dag() consults the metadata DB (DagModel.get_current) in
    Airflow 2.10; the in-memory mapping keeps this test DB-free."""
    return dagbag.dags[dag_id]


def test_all_expected_dags_load(dagbag: DagBag) -> None:
    assert set(dagbag.dag_ids) == set(EXPECTED_SCHEDULES)


def test_schedules_match_documentation(dagbag: DagBag) -> None:
    for dag_id, cron in EXPECTED_SCHEDULES.items():
        assert get_dag(dagbag, dag_id).timetable.summary == cron


def test_batch_dags_submit_existing_jobs_to_the_cluster(dagbag: DagBag) -> None:
    expected = [
        ("energy_batch_pipeline", "transform_silver_gold", ["--since-hours", "168"]),
        ("energy_history_backfill", "backfill_recent_weeks", ["--weeks", "2"]),
        ("energy_news_ingest", "ingest_news", []),
    ]
    for dag_id, task_id, application_args in expected:
        task = get_dag(dagbag, dag_id).get_task(task_id)

        # Provider 5.0.0 stores the connection as a protected attribute; there
        # is no public `conn_id` property on SparkSubmitOperator.
        assert task._conn_id == "spark_default"
        assert task.application_args == application_args
        # The task must point at a job that exists in spark/jobs/.
        assert (JOBS_DIR / Path(task.application).name).is_file()
        # Regression guard for the Airflow 2.10 provider: `packages` must be a
        # comma-separated STRING, a list breaks spark-submit command masking.
        assert isinstance(task.packages, str)
        assert "iceberg-spark-runtime" in task.packages


def test_healthcheck_dag_has_one_freshness_task(dagbag: DagBag) -> None:
    dag = get_dag(dagbag, "energy_healthcheck")
    assert [task.task_id for task in dag.tasks] == ["check_serving_freshness"]
    assert dag.default_args["retries"] == 1
