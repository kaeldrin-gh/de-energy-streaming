"""Recovery when storage is wiped but the Iceberg catalog survives.

In the stack the catalog is Postgres and the files are in LocalStack, on
separate volumes. Losing only the files used to make every job fail on start
("Failed to open input stream" for a metadata file). ``ensure_tables`` must:

* drop catalog entries whose table directory is gone and recreate the tables,
* leave tables that still have their files untouched.

Uses a SQLite-backed JDBC catalog, the same catalog type as the Postgres one.
Requires a JVM and PySpark (``pip install -e ".[sparklocal]"``); skipped
automatically when either is missing. CI runs it next to the MERGE suite.
"""

from __future__ import annotations

import shutil

import pytest

pytest.importorskip("pyspark", reason="install pyspark (.[sparklocal]) to run Spark tests")

if shutil.which("java") is None:
    pytest.skip("no JVM on PATH", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs import common  # noqa: E402
from tests.iceberg_local import local_iceberg_session  # noqa: E402

ROW = "('DE-LU', TIMESTAMP '2026-01-06 00:00:00', 42.0, 'smard', TIMESTAMP '2026-01-06 23:00:00')"


def fresh_session(tmp_path) -> SparkSession:
    """A new job process: nothing cached, catalog and warehouse read from disk."""
    return local_iceberg_session(
        "test_catalog_recovery", tmp_path / "warehouse", jdbc_catalog=tmp_path / "catalog.db"
    )


def test_wiped_storage_is_recovered_on_the_next_start(tmp_path) -> None:
    spark = fresh_session(tmp_path)
    common.ensure_tables(spark)
    spark.sql(f"INSERT INTO {common.BRONZE} VALUES {ROW}")
    spark.stop()

    # The LocalStack volume is lost; the Postgres catalog is not.
    shutil.rmtree(tmp_path / "warehouse")

    spark = fresh_session(tmp_path)
    try:
        common.ensure_tables(spark)  # used to fail on the missing metadata file

        assert spark.table(common.BRONZE).count() == 0
        spark.sql(f"INSERT INTO {common.BRONZE} VALUES {ROW}")
        assert spark.table(common.BRONZE).count() == 1
    finally:
        spark.stop()


def test_tables_with_files_are_left_alone(tmp_path) -> None:
    spark = fresh_session(tmp_path)
    common.ensure_tables(spark)
    spark.sql(f"INSERT INTO {common.BRONZE} VALUES {ROW}")
    spark.stop()

    spark = fresh_session(tmp_path)
    try:
        assert common.drop_tables_without_storage(spark) == []
        common.ensure_tables(spark)
        assert spark.table(common.BRONZE).count() == 1
    finally:
        spark.stop()
