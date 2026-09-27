"""Revision-aware MERGE tests against a real (local) Spark + Iceberg session.

This is the executable form of ADR 0002. Three properties must hold:

* replaying the same batch converges to one row per ``(region, delivery_ts)``,
* a newer revision (higher ``fetched_at``) replaces the stored row,
* an older revision can never regress a newer one.

Requires a JVM and PySpark (``pip install -e ".[sparklocal]"``); skipped
automatically when either is missing. CI runs it on every push; the session
comes from ``tests/iceberg_local.py``.
"""

from __future__ import annotations

import datetime as dt
import shutil

import pytest

pytest.importorskip("pyspark", reason="install pyspark (.[sparklocal]) to run Spark tests")

if shutil.which("java") is None:
    pytest.skip("no JVM on PATH", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from spark.jobs import common  # noqa: E402
from tests.iceberg_local import local_iceberg_session  # noqa: E402

BRONZE_SCHEMA = (
    "region string, delivery_ts timestamp, price_eur_mwh double, "
    "source string, fetched_at timestamp"
)
HOUR = dt.timedelta(hours=1)


@pytest.fixture(scope="module")
def spark(tmp_path_factory) -> SparkSession:
    """Local session with a Hadoop-catalog Iceberg warehouse in a temp dir."""
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = local_iceberg_session("test_bronze_merge", warehouse)
    common.ensure_tables(session)
    yield session
    session.stop()


def apply_merge(spark: SparkSession, rows: list[tuple], view: str) -> None:
    """Drive the same code path as the streaming job and the backfill."""
    spark.createDataFrame(rows, BRONZE_SCHEMA).createOrReplaceTempView(view)
    common.merge_bronze_from_view(spark, view)
    spark.catalog.dropTempView(view)


def stored_rows(spark: SparkSession, delivery_ts: dt.datetime):
    return (
        spark.table(common.BRONZE)
        .where(F.col("region") == "DE-LU")
        .where(F.col("delivery_ts") == delivery_ts)
        .collect()
    )


def test_replaying_the_same_batch_converges(spark: SparkSession) -> None:
    base = dt.datetime(2026, 1, 4, 0)
    rows = [
        ("DE-LU", base, 100.0, "replay", dt.datetime(2026, 1, 4, 8)),
        ("DE-LU", base + HOUR, 50.0, "replay", dt.datetime(2026, 1, 4, 8)),
    ]
    apply_merge(spark, rows, "batch_one")
    apply_merge(spark, rows, "batch_two")

    stored = (
        spark.table(common.BRONZE).where(F.col("delivery_ts").between(base, base + HOUR)).collect()
    )
    assert len(stored) == 2
    assert sorted(row.price_eur_mwh for row in stored) == [50.0, 100.0]


def test_newer_revision_updates_the_row(spark: SparkSession) -> None:
    ts = dt.datetime(2026, 1, 5, 12)
    apply_merge(
        spark,
        [("DE-LU", ts, 100.0, "smard", dt.datetime(2026, 1, 5, 10))],
        "revision_old",
    )
    apply_merge(
        spark,
        [("DE-LU", ts, 120.0, "smard_correction", dt.datetime(2026, 1, 5, 11))],
        "revision_new",
    )

    stored = stored_rows(spark, ts)
    assert len(stored) == 1
    assert stored[0].price_eur_mwh == 120.0
    assert stored[0].source == "smard_correction"


def test_older_revision_cannot_overwrite_newer(spark: SparkSession) -> None:
    ts = dt.datetime(2026, 1, 5, 13)
    apply_merge(
        spark,
        [("DE-LU", ts, 120.0, "smard", dt.datetime(2026, 1, 5, 11))],
        "fresh_first",
    )
    apply_merge(
        spark,
        [("DE-LU", ts, 100.0, "stale_replay", dt.datetime(2026, 1, 5, 10))],
        "stale_second",
    )

    stored = stored_rows(spark, ts)
    assert len(stored) == 1
    assert stored[0].price_eur_mwh == 120.0
    assert stored[0].source == "smard"


def test_duplicates_inside_one_batch_keep_the_newest(spark: SparkSession) -> None:
    ts = dt.datetime(2026, 1, 5, 14)
    rows = [
        ("DE-LU", ts, 90.0, "replay", dt.datetime(2026, 1, 5, 9)),
        ("DE-LU", ts, 95.0, "replay", dt.datetime(2026, 1, 5, 10)),
    ]
    apply_merge(spark, rows, "duplicates")

    stored = stored_rows(spark, ts)
    assert len(stored) == 1
    assert stored[0].price_eur_mwh == 95.0
