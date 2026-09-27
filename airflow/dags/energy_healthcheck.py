"""Pipeline health checks.

Checks that the serving layer is fresh, records the result in
``serving.pipeline_health`` (surfaced on the Grafana dashboard), and fails the
task when the pipeline is unhealthy - so Airflow alerts work out of the box.
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

BERLIN = ZoneInfo("Europe/Berlin")

# SMARD publishes a delivery day's prices around 13:00 the afternoon before. The
# 03:00 backfill loads them even when no live producer runs, and the 04:00 hourly
# transform publishes them to serving, so from 05:00 local every hour of today is
# due. Before 05:00 only yesterday is. A live producer loads days earlier; that
# only makes the check pass sooner, never fail.
TODAY_DUE_FROM = dt.time(5, 0)


def due_through(now: dt.datetime) -> dt.datetime:
    """Start of the last delivery hour (23:00 Berlin) that serving must contain."""
    local = now.astimezone(BERLIN)
    day = local.date() if local.time() >= TODAY_DUE_FROM else local.date() - dt.timedelta(days=1)
    return dt.datetime.combine(day, dt.time(23, 0), tzinfo=BERLIN)


def _local(ts: dt.datetime) -> str:
    return ts.astimezone(BERLIN).strftime("%Y-%m-%d %H:%M %Z")


def describe_freshness(latest: dt.datetime | None, now: dt.datetime) -> tuple[str, str]:
    """Status and a readable detail for the newest delivery hour in serving.

    Fresh means every hour that is already due is present (see TODAY_DUE_FROM),
    not that the newest hour is recent: day-ahead hours are usually in the
    future, so an age limit would miss a stopped pipeline for most of a day.
    """
    if latest is None:
        return "fail", "serving.price_hourly is empty"
    due = due_through(now)
    hours = (latest - now).total_seconds() / 3600
    position = f"{hours:.1f} h ahead" if hours >= 0 else f"{-hours:.1f} h old"
    if latest >= due:
        return "ok", f"newest hour {_local(latest)} ({position}); due through {_local(due)}"
    return "fail", (
        f"newest hour {_local(latest)} ({position}); hours through {_local(due)} are due "
        f"and missing (the 03:00 backfill or the hourly transform did not publish them)"
    )


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
