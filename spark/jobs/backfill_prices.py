"""Batch backfill: SMARD.de -> Iceberg bronze.

Same MERGE semantics as the streaming job, so history can be re-ingested at
any time without regressing newer revisions.

Run on the cluster (or via the Airflow DAG ``energy_history_backfill``):

    spark-submit --master spark://spark-master:7077 /opt/jobs/backfill_prices.py --weeks 4
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from common import build_spark, ensure_tables, env, merge_bronze_from_view

from producer.smard import SmardClient

BRONZE_SCHEMA = (
    "region string, delivery_ts timestamp, price_eur_mwh double, "
    "source string, fetched_at timestamp"
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--weeks", type=int, default=4, help="number of recent weekly chunks to load"
    )
    args, _ = parser.parse_known_args()

    spark = build_spark("backfill_prices")
    ensure_tables(spark)

    client = SmardClient(
        base_url=env("SMARD_BASE_URL", "https://www.smard.de/app/chart_data"),
        filter_id=env("SMARD_FILTER", "4169"),
        region=env("SMARD_REGION", "DE-LU"),
    )
    points = client.fetch_latest(weeks=args.weeks)
    if not points:
        raise SystemExit("SMARD returned no published points; nothing to backfill")

    fetched_at = datetime.now(UTC)
    rows = [
        (point.region, point.delivery_ts_utc, point.price_eur_mwh, "smard_backfill", fetched_at)
        for point in points
    ]
    frame = spark.createDataFrame(rows, BRONZE_SCHEMA)
    frame.createOrReplaceTempView("backfill_batch")
    merge_bronze_from_view(spark, "backfill_batch")
    spark.catalog.dropTempView("backfill_batch")

    first, last = rows[0][1], rows[-1][1]
    print(f"backfill complete: {len(rows)} points, {first} .. {last} (weeks={args.weeks})")


if __name__ == "__main__":
    main()
