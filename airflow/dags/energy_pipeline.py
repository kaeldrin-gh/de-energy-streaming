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

# The jobs run in client mode, so the Spark driver lives inside this Airflow
# container and needs the Iceberg/Kafka/S3A/Postgres jars on its classpath.
# Spark resolves them from Maven Central on first run and caches them in the
# `ivy-cache` volume (mounted at /home/airflow/.ivy2), then ships them to the
# workers, which already have them baked into the image.
SPARK_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.3",
]

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
        packages=SPARK_PACKAGES,
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
