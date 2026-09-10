"""Shared Spark/Iceberg helpers for the streaming and batch jobs.

Catalog layout (one Iceberg namespace, medallion layers):

    lake.energy.bronze_prices   raw observations, revision-aware upserts
    lake.energy.silver_prices   deduplicated + time-enriched (Europe/Berlin)
    lake.energy.gold_daily      daily aggregates used by the serving layer
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

CATALOG = "lake"
NAMESPACE = "energy"
BRONZE = f"{NAMESPACE}.bronze_prices"
SILVER = f"{NAMESPACE}.silver_prices"
GOLD_DAILY = f"{NAMESPACE}.gold_daily"


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
        .config(
            f"spark.sql.catalog.{CATALOG}.warehouse",
            env("ICEBERG_WAREHOUSE", "s3a://energy-lake/warehouse"),
        )
        .config("spark.sql.defaultCatalog", CATALOG)
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


def ensure_tables(spark: SparkSession) -> None:
    """Create namespace and tables if missing (idempotent; safe on every job start)."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

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
