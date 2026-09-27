"""Shared Spark/Iceberg helpers for the streaming and batch jobs.

Catalog layout (one Iceberg namespace, medallion layers):

    lake.energy.bronze_prices   raw observations, revision-aware upserts
    lake.energy.silver_prices   deduplicated + time-enriched (Europe/Berlin)
    lake.energy.gold_daily      daily aggregates used by the serving layer
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession

CATALOG = "lake"
# Fully qualified: the session's default catalog stays spark_catalog so that
# temporary views created in foreachBatch resolve normally; Iceberg tables are
# addressed via their 3-part names instead.
NAMESPACE = f"{CATALOG}.energy"
BRONZE = f"{NAMESPACE}.bronze_prices"
SILVER = f"{NAMESPACE}.silver_prices"
GOLD_DAILY = f"{NAMESPACE}.gold_daily"
NEWS = f"{NAMESPACE}.news_events"
ALL_TABLES = (BRONZE, SILVER, GOLD_DAILY, NEWS)


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def build_spark(app_name: str) -> SparkSession:
    """Spark session wired for Iceberg (JDBC catalog) + S3A (LocalStack/AWS)."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "jdbc")
        .config(
            f"spark.sql.catalog.{CATALOG}.uri",
            env("ICEBERG_CATALOG_URI", "jdbc:postgresql://postgres:5432/iceberg"),
        )
        .config(f"spark.sql.catalog.{CATALOG}.jdbc.user", env("POSTGRES_USER", "energy"))
        .config(f"spark.sql.catalog.{CATALOG}.jdbc.password", env("POSTGRES_PASSWORD", "energy"))
        # V1 keeps the JDBC catalog schema current (enables catalog-level view
        # support; migrated on first connect). Iceberg tables are addressed by
        # 3-part name - the session default catalog must stay spark_catalog so
        # temp views used inside MERGE resolve normally.
        .config(f"spark.sql.catalog.{CATALOG}.jdbc.schema-version", "V1")
        .config(
            f"spark.sql.catalog.{CATALOG}.warehouse",
            env("ICEBERG_WAREHOUSE", "s3a://energy-lake/warehouse"),
        )
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        # S3A against LocalStack (path-style, no TLS, static test credentials)
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT", "http://localstack:4566"))
        .config("spark.hadoop.fs.s3a.access.key", env("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", env("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def drop_tables_without_storage(spark: SparkSession) -> list[str]:
    """Forget catalog entries whose table directory no longer exists in storage.

    The catalog (Postgres) and the table files (LocalStack S3) live in separate
    volumes. If storage is wiped but the catalog survives, every job fails on
    start with "Failed to open input stream" for a metadata file that is gone.
    A table whose whole directory is missing has nothing left to recover, so
    its entry is dropped without purging (the catalog never reads the missing
    metadata) and ``ensure_tables`` recreates it empty; the stream then rebuilds
    bronze from Kafka. Tables with any files left are never touched.

    Relies on the tables living at the catalog's default location
    (``<warehouse>/<namespace>/<table>``), which ``ensure_tables`` guarantees.
    """
    namespace = NAMESPACE.split(".", 1)[1]
    warehouse = spark.conf.get(f"spark.sql.catalog.{CATALOG}.warehouse").rstrip("/")
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    iceberg_catalog = (
        spark._jsparkSession.sessionState().catalogManager().catalog(CATALOG).icebergCatalog()
    )

    dropped = []
    for row in spark.sql(f"SHOW TABLES IN {NAMESPACE}").collect():
        table_dir = jvm.org.apache.hadoop.fs.Path(f"{warehouse}/{namespace}/{row.tableName}")
        if table_dir.getFileSystem(hadoop_conf).exists(table_dir):
            continue
        identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.parse(
            f"{namespace}.{row.tableName}"
        )
        iceberg_catalog.dropTable(identifier, False)
        dropped.append(f"{NAMESPACE}.{row.tableName}")
        print(f"{NAMESPACE}.{row.tableName}: files missing from storage, catalog entry dropped")
    return dropped


def ensure_tables(spark: SparkSession) -> None:
    """Create namespace and tables if missing (idempotent; safe on every job start).

    Catalog entries left behind by wiped storage are dropped first
    (``drop_tables_without_storage``), so a reset LocalStack volume does not
    stop every job.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    drop_tables_without_storage(spark)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            region        string,
            delivery_ts   timestamp,
            price_eur_mwh double,
            source        string,
            fetched_at    timestamp
        ) USING iceberg
        PARTITIONED BY (days(delivery_ts))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER} (
            region        string,
            delivery_ts   timestamp,
            price_eur_mwh double,
            local_ts      timestamp,
            local_hour    int,
            is_weekend    boolean,
            is_negative   boolean,
            source        string,
            updated_at    timestamp
        ) USING iceberg
        PARTITIONED BY (days(delivery_ts))
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DAILY} (
            region         string,
            local_day      date,
            avg_price      double,
            min_price      double,
            max_price      double,
            negative_hours int,
            updated_at     timestamp
        ) USING iceberg
        PARTITIONED BY (local_day)
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {NEWS} (
            source       string,
            link         string,
            title        string,
            published_ts timestamp,
            category     string,
            confidence   double,
            model        string,
            fetched_at   timestamp
        ) USING iceberg
        PARTITIONED BY (days(published_ts))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def merge_bronze_from_view(spark: SparkSession, view: str) -> None:
    """Revision-aware upsert: newer ``fetched_at`` wins, one row per natural key.

    This is the single place where ingestion correctness is defined - both the
    streaming micro-batches and the batch backfill call it (see ADR 0002).
    """
    spark.sql(
        f"""
        MERGE INTO {BRONZE} AS t
        USING (
            SELECT region, delivery_ts, price_eur_mwh, source, fetched_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY region, delivery_ts ORDER BY fetched_at DESC
                       ) AS rn
                FROM {view}
            ) WHERE rn = 1
        ) AS s
        ON t.region = s.region AND t.delivery_ts = s.delivery_ts
        WHEN MATCHED AND s.fetched_at > t.fetched_at THEN UPDATE SET
            price_eur_mwh = s.price_eur_mwh,
            source        = s.source,
            fetched_at    = s.fetched_at
        WHEN NOT MATCHED THEN INSERT
            (region, delivery_ts, price_eur_mwh, source, fetched_at)
            VALUES (s.region, s.delivery_ts, s.price_eur_mwh, s.source, s.fetched_at)
        """
    )


def merge_news_from_view(spark: SparkSession, view: str) -> None:
    """News upsert: one row per (source, link), newer ``fetched_at`` wins.

    Reclassification on a later run updates the category; a transient classifier
    outage leaves it NULL until then (ADR 0003).
    """
    spark.sql(
        f"""
        MERGE INTO {NEWS} AS t
        USING (
            SELECT source, link, title, published_ts, category, confidence, model, fetched_at
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY source, link ORDER BY fetched_at DESC
                       ) AS rn
                FROM {view}
            ) WHERE rn = 1
        ) AS s
        ON t.source = s.source AND t.link = s.link
        WHEN MATCHED AND s.fetched_at > t.fetched_at THEN UPDATE SET
            title        = s.title,
            published_ts = s.published_ts,
            category     = s.category,
            confidence   = s.confidence,
            model        = s.model,
            fetched_at   = s.fetched_at
        WHEN NOT MATCHED THEN INSERT
            (source, link, title, published_ts, category, confidence, model, fetched_at)
            VALUES (s.source, s.link, s.title, s.published_ts, s.category, s.confidence,
                    s.model, s.fetched_at)
        """
    )


def maintain_table(
    spark: SparkSession,
    table: str,
    older_than: datetime,
    retain_last: int,
    min_input_files: int = 5,
) -> dict[str, int]:
    """Compact small data files, then expire snapshots older than ``older_than``.

    The streaming job commits one snapshot and at least one small file per
    micro-batch, so without this the metadata and file counts grow without
    bound. Compaction rewrites files but never changes rows; expiry keeps at
    least ``retain_last`` snapshots (and everything newer than ``older_than``)
    for time travel. Partial progress lets compaction commit the partitions it
    finished even if a concurrent streaming write conflicts with another one.

    ``older_than`` must be timezone-aware; it is sent as UTC (``Z`` suffix) so
    the cutoff does not depend on the Spark session time zone.
    """
    if older_than.tzinfo is None:
        raise ValueError("older_than must be timezone-aware")
    cutoff = older_than.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    identifier = table.split(".", 1)[1]  # procedures take `namespace.table`
    rewrite = spark.sql(
        f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{identifier}',
            options => map(
                'min-input-files', '{int(min_input_files)}',
                'partial-progress.enabled', 'true'
            )
        )
        """
    ).first()
    expire = spark.sql(
        f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{identifier}',
            older_than => TIMESTAMP '{cutoff}',
            retain_last => {int(retain_last)}
        )
        """
    ).first()
    return {
        "rewritten_files": int(rewrite.rewritten_data_files_count),
        "added_files": int(rewrite.added_data_files_count),
        "deleted_files": int(expire.deleted_data_files_count),
    }
