"""Pipeline health checks.

Checks that the serving layer is fresh, records the result in
``serving.pipeline_health`` (surfaced on the Grafana dashboard), and fails the
task when the pipeline is unhealthy - so Airflow alerts work out of the box.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

FRESHNESS_SLA_HOURS = 3


def describe_freshness(
    latest: dt.datetime | None, now: dt.datetime, sla_hours: float = FRESHNESS_SLA_HOURS
) -> tuple[str, str]:
    """Status and a readable detail for the newest delivery hour in serving.

    Day-ahead prices are published around 13:00 for the whole next day, so the
    newest hour is usually in the future. That is reported as "ahead" rather than
    as a negative age ("-4.8h old"), which read like a clock bug.
    """
    if latest is None:
        return "fail", "serving.price_hourly is empty"
    hours = (now - latest).total_seconds() / 3600
    when = latest.astimezone(dt.UTC).strftime("%Y-%m-%d %H:%M UTC")
    if hours <= 0:
        return "ok", f"newest hour {when} is {-hours:.1f} h ahead (published day-ahead)"
    if hours <= sla_hours:
        return "ok", f"newest hour {when} is {hours:.1f} h old"
    return "fail", f"newest hour {when} is {hours:.1f} h old, over the {sla_hours:g} h limit"


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
        status, detail = describe_freshness(latest, dt.datetime.now(dt.UTC))

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
