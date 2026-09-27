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
    "energy_table_maintenance": "30 4 * * *",
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
        (
            "energy_table_maintenance",
            "maintain_iceberg_tables",
            ["--retain-days", "7", "--retain-last", "10"],
        ),
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


def _healthcheck_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "energy_healthcheck", DAGS_DIR / "energy_healthcheck.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_freshness_requires_every_hour_that_is_already_due() -> None:
    """Fresh = today's hours are in serving from 05:00 Berlin (yesterday's before).

    Day-ahead hours are usually in the future, so an age limit on the newest hour
    missed a stopped pipeline for most of a day and failed every night when only
    the 03:00 backfill loads prices.
    """
    import datetime as dt

    describe = _healthcheck_module().describe_freshness
    utc = dt.UTC
    today_end = dt.datetime(2026, 9, 27, 21, 0, tzinfo=utc)  # 27 Sep 23:00 CEST
    tomorrow_end = dt.datetime(2026, 9, 28, 21, 0, tzinfo=utc)  # 28 Sep 23:00 CEST

    # Evening with tomorrow already published (live producer running).
    status, detail = describe(tomorrow_end, dt.datetime(2026, 9, 27, 16, 10, tzinfo=utc))
    assert status == "ok"
    assert detail == (
        "newest hour 2026-09-28 23:00 CEST (28.8 h ahead); due through 2026-09-27 23:00 CEST"
    )

    # Same evening, only today loaded (Airflow backfill only): still fresh.
    assert describe(today_end, dt.datetime(2026, 9, 27, 16, 10, tzinfo=utc))[0] == "ok"

    # 04:30 next morning, before the backfilled day is due: fresh, no nightly alarm.
    assert describe(today_end, dt.datetime(2026, 9, 28, 2, 30, tzinfo=utc))[0] == "ok"

    # 05:30 next morning and today's hours are still missing: stale.
    status, detail = describe(today_end, dt.datetime(2026, 9, 28, 3, 30, tzinfo=utc))
    assert status == "fail"
    assert detail.startswith("newest hour 2026-09-27 23:00 CEST (6.5 h old); hours through")
    assert "2026-09-28 23:00 CEST are due and missing" in detail

    assert describe(None, dt.datetime(2026, 9, 27, 12, 0, tzinfo=utc)) == (
        "fail",
        "serving.price_hourly is empty",
    )


def test_freshness_due_hour_follows_berlin_time_across_dst() -> None:
    import datetime as dt

    module = _healthcheck_module()
    # 25 Oct 2026 ends CET (UTC+1): its last hour starts 22:00 UTC, not 21:00.
    due = module.due_through(dt.datetime(2026, 10, 25, 9, 0, tzinfo=dt.UTC))
    assert due.astimezone(dt.UTC) == dt.datetime(2026, 10, 25, 22, 0, tzinfo=dt.UTC)
