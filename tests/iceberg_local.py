"""Local Spark + Iceberg session shared by the Spark test suites.

Imported only after the suites have checked for PySpark and a JVM. The
Iceberg runtime jar (the same one the Spark image bakes in) is downloaded once
into ``~/.cache/de-energy-streaming``.
"""

from __future__ import annotations

import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession

ICEBERG_JAR_NAME = "iceberg-spark-runtime-3.5_2.12-1.8.1.jar"
ICEBERG_JAR_URL = (
    "https://repo1.maven.org/maven2/org/apache/iceberg/"
    f"iceberg-spark-runtime-3.5_2.12/1.8.1/{ICEBERG_JAR_NAME}"
)


# A JDBC catalog like the Postgres one in docker-compose, backed by a SQLite
# file so the catalog and the warehouse can be lost independently in tests.
SQLITE_JAR_NAME = "sqlite-jdbc-3.46.1.3.jar"
SQLITE_JAR_URL = f"https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.46.1.3/{SQLITE_JAR_NAME}"


def cached_jar(name: str, url: str) -> str:
    """Download a jar once; return it as a file: URI.

    Spark's ``spark.jars.packages`` needs an Ivy-enabled spark-submit, which a
    programmatic session does not provide; a local jar keeps the test offline
    after the first download and avoids Ivy in CI.
    """
    cache = Path.home() / ".cache" / "de-energy-streaming"
    cache.mkdir(parents=True, exist_ok=True)
    jar = cache / name
    if not jar.exists():
        urllib.request.urlretrieve(url, jar)
    return jar.as_uri()


def local_iceberg_session(
    app_name: str, warehouse: Path, jdbc_catalog: Path | None = None
) -> SparkSession:
    """Local session with an Iceberg catalog named ``lake``.

    Hadoop catalog by default (tables live only in the warehouse); pass
    ``jdbc_catalog`` for a SQLite-backed JDBC catalog, the same catalog type
    the stack runs on Postgres.
    """
    # Both jars on every session: PySpark keeps one JVM per test process, and a
    # JDBC driver added by a later session is invisible to DriverManager.
    jars = [
        cached_jar(ICEBERG_JAR_NAME, ICEBERG_JAR_URL),
        cached_jar(SQLITE_JAR_NAME, SQLITE_JAR_URL),
    ]
    catalog = {"type": "hadoop"}
    if jdbc_catalog is not None:
        catalog = {
            "type": "jdbc",
            "uri": f"jdbc:sqlite:{jdbc_catalog}",
            "jdbc.schema-version": "V1",
        }

    builder = (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars", ",".join(jars))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.warehouse", warehouse.as_uri())
    )
    for key, value in catalog.items():
        builder = builder.config(f"spark.sql.catalog.lake.{key}", value)
    return builder.getOrCreate()
