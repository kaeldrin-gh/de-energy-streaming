"""Pipeline health checks.

Checks that the serving layer is fresh, records the result in
``serving.pipeline_health`` (surfaced on the Grafana dashboard), and fails the
task when the pipeline is unhealthy - so Airflow alerts work out of the box.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

FRESHNESS_SLA_HOURS = 3


@dag(
    dag_id="energy_healthcheck",
    description="Freshness check for the serving layer; writes pipeline_health.",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Berlin"),
    catchup=False,
    tags=["energy", "ops"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
)
def energy_healthcheck():
    @task
    def check_serving_freshness() -> dict:
        hook = PostgresHook(postgres_conn_id="serving")
        row = hook.get_first("SELECT max(delivery_ts) FROM serving.price_hourly")
        latest = row[0] if row else None

        if latest is None:
            status, detail = "fail", "serving.price_hourly is empty"
        else:
            age_hours = (
                pendulum.now("UTC") - pendulum.instance(latest).in_timezone("UTC")
            ).total_hours()
            if age_hours <= FRESHNESS_SLA_HOURS:
                status, detail = "ok", f"latest hour {latest.isoformat()} ({age_hours:.1f}h old)"
            else:
                status, detail = "fail", f"latest hour {latest.isoformat()} is {age_hours:.1f}h old"

        hook.run(
            """
            INSERT INTO serving.pipeline_health (check_name, status, detail, checked_at)
            VALUES ('serving_freshness', %s, %s, now())
            ON CONFLICT (check_name) DO UPDATE SET
                status = EXCLUDED.status, detail = EXCLUDED.detail, checked_at = EXCLUDED.checked_at
            """,
            parameters=(status, detail),
        )

        if status != "ok":
            raise AirflowException(detail)
        return {"status": status, "detail": detail}

    check_serving_freshness()


energy_healthcheck()
