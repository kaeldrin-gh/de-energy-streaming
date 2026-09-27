"""Iceberg maintenance tests against a real (local) Spark + Iceberg session.

``maintain_table`` (run daily by ``spark/jobs/maintain_tables.py``) must:

* compact many small data files into fewer without changing a single row,
* expire snapshots outside the retention window but never inside it,
* be safe to re-run: a compacted table is left alone.

Requires a JVM and PySpark (``pip install -e ".[sparklocal]"``); skipped
automatically when either is missing. CI runs it next to the MERGE suite.
"""

from __future__ import annotations

import datetime as dt
import shutil

import pytest

pytest.importorskip("pyspark", reason="install pyspark (.[sparklocal]) to run Spark tests")

if shutil.which("java") is None:
    pytest.skip("no JVM on PATH", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs import common  # noqa: E402
from tests.iceberg_local import local_iceberg_session  # noqa: E402

DAY = dt.datetime(2026, 1, 6)
FAR_FUTURE = dt.datetime(2100, 1, 1, tzinfo=dt.UTC)
FAR_PAST = dt.datetime(2000, 1, 1, tzinfo=dt.UTC)


@pytest.fixture()
def spark(tmp_path) -> SparkSession:
    """A fresh warehouse per test, so file and snapshot counts start at zero."""
    session = local_iceberg_session("test_table_maintenance", tmp_path / "warehouse")
    common.ensure_tables(session)
    yield session
    session.stop()


def write_hourly_commits(spark: SparkSession, hours: int) -> None:
    """One INSERT per hour, as the streaming job does: one small file each."""
    for hour in range(hours):
        spark.sql(
            f"""
            INSERT INTO {common.BRONZE} VALUES (
                'DE-LU', TIMESTAMP '{DAY + dt.timedelta(hours=hour):%Y-%m-%d %H:%M:%S}',
                {50.0 + hour}, 'smard', TIMESTAMP '2026-01-06 23:00:00'
            )
            """
        )


def data_files(spark: SparkSession) -> int:
    return spark.sql(f"SELECT count(*) FROM {common.BRONZE}.files").first()[0]


def snapshots(spark: SparkSession) -> int:
    return spark.sql(f"SELECT count(*) FROM {common.BRONZE}.snapshots").first()[0]


def rows(spark: SparkSession) -> list[tuple]:
    return sorted(
        tuple(row)
        for row in spark.table(common.BRONZE).select("delivery_ts", "price_eur_mwh").collect()
    )


def test_compaction_merges_small_files_without_changing_rows(spark: SparkSession) -> None:
    write_hourly_commits(spark, 4)
    before = rows(spark)
    assert data_files(spark) == 4

    stats = common.maintain_table(spark, common.BRONZE, FAR_PAST, retain_last=1, min_input_files=2)

    assert stats["rewritten_files"] == 4
    assert stats["added_files"] == 1
    assert data_files(spark) == 1
    assert rows(spark) == before


def test_expiry_keeps_snapshots_inside_the_retention_window(spark: SparkSession) -> None:
    write_hourly_commits(spark, 3)
    assert snapshots(spark) == 3

    # Nothing is older than the cutoff: compaction adds a snapshot, none expire.
    common.maintain_table(spark, common.BRONZE, FAR_PAST, retain_last=1, min_input_files=2)
    assert snapshots(spark) == 4

    # Everything is older than the cutoff: only the newest (`retain_last`) stays.
    stats = common.maintain_table(
        spark, common.BRONZE, FAR_FUTURE, retain_last=1, min_input_files=2
    )
    assert snapshots(spark) == 1
    # Only now is no snapshot left that references the pre-compaction files.
    assert stats["deleted_files"] == 3


def test_rerun_on_a_compacted_table_rewrites_nothing(spark: SparkSession) -> None:
    write_hourly_commits(spark, 3)
    common.maintain_table(spark, common.BRONZE, FAR_PAST, retain_last=1, min_input_files=2)

    stats = common.maintain_table(spark, common.BRONZE, FAR_PAST, retain_last=1, min_input_files=2)

    assert stats["rewritten_files"] == 0
    assert data_files(spark) == 1


def test_older_than_must_be_timezone_aware(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        common.maintain_table(spark, common.BRONZE, dt.datetime(2026, 1, 1), retain_last=1)
