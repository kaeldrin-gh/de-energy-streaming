"""Batch orchestration for the pipeline.

* ``energy_batch_pipeline``  - hourly: transform bronze -> silver -> gold, refresh serving tables.
* ``energy_history_backfill`` - daily: revision-aware backfill of recent SMARD weeks.

Both tasks submit the same jobs that can be run manually with spark-submit,
so Airflow owns scheduling/retries, not business logic.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
JOBS_DIR = "/opt/jobs"

DEFAULT_ARGS = {"retries": 2, "retry_delay": pendulum.duration(minutes=2)}


def spark_task(
    task_id: str, application: str, application_args: list[str] | None = None
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=application,
        name=task_id,
        application_args=application_args or [],
        verbose=False,
    )


@dag(
    dag_id="energy_batch_pipeline",
    description="Hourly bronze -> silver -> gold transform and serving refresh.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Berlin"),
    catchup=False,
    max_active_runs=1,
    tags=["energy", "batch"],
    default_args=DEFAULT_ARGS,
)
def energy_batch_pipeline():
    spark_task("transform_silver_gold", f"{JOBS_DIR}/transform_silver.py", ["--since-hours", "168"])


@dag(
    dag_id="energy_history_backfill",
    description="Daily revision-aware backfill of recent SMARD.de weeks.",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Berlin"),
    catchup=False,
    max_active_runs=1,
    tags=["energy", "batch"],
    default_args=DEFAULT_ARGS,
)
def energy_history_backfill():
    spark_task("backfill_recent_weeks", f"{JOBS_DIR}/backfill_prices.py", ["--weeks", "2"])


energy_batch_pipeline()
energy_history_backfill()
