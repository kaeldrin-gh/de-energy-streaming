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


def cached_iceberg_jar() -> str:
    """Download the Iceberg runtime bundle once; return it as a file: URI.

    Spark's ``spark.jars.packages`` needs an Ivy-enabled spark-submit, which a
    programmatic session does not provide; a local jar keeps the test offline
    after the first download and avoids Ivy in CI.
    """
    cache = Path.home() / ".cache" / "de-energy-streaming"
    cache.mkdir(parents=True, exist_ok=True)
    jar = cache / ICEBERG_JAR_NAME
    if not jar.exists():
        urllib.request.urlretrieve(ICEBERG_JAR_URL, jar)
    return jar.as_uri()


def local_iceberg_session(app_name: str, warehouse: Path) -> SparkSession:
    """Local session with a Hadoop-catalog Iceberg warehouse named ``lake``."""
    return (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars", cached_iceberg_jar())
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", warehouse.as_uri())
        .getOrCreate()
    )
