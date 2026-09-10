"""Hourly batch transform: bronze -> silver (enriched) -> gold + serving upsert.

Run on the cluster (or via the Airflow DAG ``energy_batch_pipeline``):

    spark-submit --master spark://spark-master:7077 /opt/jobs/transform_silver.py --since-hours 168

Design notes:
* Recomputes a sliding window instead of tracking a high-water mark, so
  late revisions inside the window are always picked up and the job stays
  idempotent.
* Writes small aggregates to Postgres (``serving`` schema) for Grafana and
  ad-hoc SQL - the lakehouse remains the source of truth.
"""

from __future__ import annotations

import argparse

import psycopg2
import psycopg2.extras
from common import BRONZE, GOLD_DAILY, SILVER, build_spark, ensure_tables, env
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LOCAL_TZ = "Europe/Berlin"


def enrich(deduped: DataFrame) -> DataFrame:
    return (
        deduped.withColumn("local_ts", F.from_utc_timestamp(F.col("delivery_ts"), LOCAL_TZ))
        .withColumn("local_hour", F.hour("local_ts"))
        .withColumn("is_weekend", F.dayofweek("local_ts").isin(1, 7))
        .withColumn("is_negative", F.col("price_eur_mwh") < 0)
        .withColumn("updated_at", F.current_timestamp())
    )


def upsert_serving(dsn: str, daily_rows: list[tuple], hourly_rows: list[tuple]) -> None:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO serving.daily_stats
                (region, local_day, avg_price, min_price, max_price, negative_hours, updated_at)
            VALUES %s
            ON CONFLICT (region, local_day) DO UPDATE SET
                avg_price      = EXCLUDED.avg_price,
                min_price      = EXCLUDED.min_price,
                max_price      = EXCLUDED.max_price,
                negative_hours = EXCLUDED.negative_hours,
                updated_at     = EXCLUDED.updated_at
            """,
            daily_rows,
        )
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO serving.price_hourly
                (region, delivery_ts, price_eur_mwh, local_hour,
                 is_weekend, is_negative, updated_at)
            VALUES %s
            ON CONFLICT (region, delivery_ts) DO UPDATE SET
                price_eur_mwh = EXCLUDED.price_eur_mwh,
                local_hour    = EXCLUDED.local_hour,
                is_weekend    = EXCLUDED.is_weekend,
                is_negative   = EXCLUDED.is_negative,
                updated_at    = EXCLUDED.updated_at
            """,
            hourly_rows,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since-hours", type=int, default=168, help="recompute window in hours")
    args, _ = parser.parse_known_args()

    spark = build_spark("transform_silver")
    ensure_tables(spark)

    window_start = F.expr(f"current_timestamp() - INTERVAL {int(args.since_hours)} HOURS")

    deduped = (
        spark.table(BRONZE)
        .where(F.col("delivery_ts") >= window_start)
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("region", "delivery_ts").orderBy(F.col("fetched_at").desc())
            ),
        )
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    silver_frame = enrich(deduped)
    silver_frame.createOrReplaceTempView("silver_incoming")
    spark.sql(
        f"""
        MERGE INTO {SILVER} AS t
        USING (
            SELECT region, delivery_ts, price_eur_mwh, local_ts, local_hour, is_weekend,
                   is_negative, source, updated_at
            FROM silver_incoming
        ) AS s
        ON t.region = s.region AND t.delivery_ts = s.delivery_ts
        WHEN MATCHED THEN UPDATE SET
            price_eur_mwh = s.price_eur_mwh,
            local_ts      = s.local_ts,
            local_hour    = s.local_hour,
            is_weekend    = s.is_weekend,
            is_negative   = s.is_negative,
            source        = s.source,
            updated_at    = s.updated_at
        WHEN NOT MATCHED THEN INSERT
            (region, delivery_ts, price_eur_mwh, local_ts, local_hour, is_weekend,
             is_negative, source, updated_at)
            VALUES (s.region, s.delivery_ts, s.price_eur_mwh, s.local_ts, s.local_hour,
                    s.is_weekend, s.is_negative, s.source, s.updated_at)
        """
    )
    spark.catalog.dropTempView("silver_incoming")

    daily = (
        spark.table(SILVER)
        .where(F.col("delivery_ts") >= window_start)
        .groupBy("region", F.to_date(F.col("local_ts")).alias("local_day"))
        .agg(
            F.round(F.avg("price_eur_mwh"), 2).alias("avg_price"),
            F.round(F.min("price_eur_mwh"), 2).alias("min_price"),
            F.round(F.max("price_eur_mwh"), 2).alias("max_price"),
            F.sum(F.when(F.col("is_negative"), 1).otherwise(0)).cast("int").alias("negative_hours"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    daily.createOrReplaceTempView("daily_incoming")
    spark.sql(
        f"""
        MERGE INTO {GOLD_DAILY} AS t
        USING daily_incoming AS s
        ON t.region = s.region AND t.local_day = s.local_day
        WHEN MATCHED THEN UPDATE SET
            avg_price      = s.avg_price,
            min_price      = s.min_price,
            max_price      = s.max_price,
            negative_hours = s.negative_hours,
            updated_at     = s.updated_at
        WHEN NOT MATCHED THEN INSERT
            (region, local_day, avg_price, min_price, max_price, negative_hours, updated_at)
            VALUES (s.region, s.local_day, s.avg_price, s.min_price, s.max_price,
                    s.negative_hours, s.updated_at)
        """
    )
    spark.catalog.dropTempView("daily_incoming")

    # small window -> collect to the driver and upsert into the serving DB
    hourly_rows = [
        (
            row.region,
            row.delivery_ts,
            float(row.price_eur_mwh or 0.0),
            row.local_hour,
            bool(row.is_weekend),
            bool(row.is_negative),
            row.updated_at,
        )
        for row in spark.table(SILVER).where(F.col("delivery_ts") >= window_start).collect()
    ]
    daily_rows = [
        (
            row.region,
            row.local_day,
            float(row.avg_price or 0.0),
            float(row.min_price or 0.0),
            float(row.max_price or 0.0),
            int(row.negative_hours or 0),
            row.updated_at,
        )
        for row in spark.table(GOLD_DAILY).where(F.col("updated_at") >= window_start).collect()
    ]
    upsert_serving(
        env("SERVING_DSN", "postgresql://energy:energy@postgres:5432/serving"),
        daily_rows,
        hourly_rows,
    )

    print(
        f"transform complete: {len(hourly_rows)} hourly rows, "
        f"{len(daily_rows)} daily rows in serving"
    )


if __name__ == "__main__":
    main()
