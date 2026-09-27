"""Daily Iceberg maintenance: compact small files and expire old snapshots.

Every streaming micro-batch and every hourly MERGE commits a new snapshot and
small data files; left alone, planning gets slower and storage keeps every
superseded file forever. This job runs ``rewrite_data_files`` and
``expire_snapshots`` on each table in the namespace (``maintain_table`` in
``common.py``), keeping a week of snapshots for time travel by default.

Run on the cluster (or via the Airflow DAG ``energy_table_maintenance``):

    spark-submit --master spark://spark-master:7077 /opt/jobs/maintain_tables.py --retain-days 7
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from common import ALL_TABLES, build_spark, ensure_tables, maintain_table

# The Spark containers run Python 3.10 (Ubuntu 22.04); datetime.UTC exists only
# in 3.11+. Keep this alias and leave UP017 disabled for spark/jobs (pyproject).
UTC = timezone.utc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--retain-days", type=int, default=7, help="expire snapshots older than this"
    )
    parser.add_argument(
        "--retain-last", type=int, default=10, help="always keep this many snapshots"
    )
    args, _ = parser.parse_known_args()

    spark = build_spark("maintain_tables")
    ensure_tables(spark)

    older_than = datetime.now(UTC) - timedelta(days=args.retain_days)
    for table in ALL_TABLES:
        stats = maintain_table(spark, table, older_than, args.retain_last)
        print(
            f"{table}: compacted {stats['rewritten_files']} files into "
            f"{stats['added_files']}, expiry deleted {stats['deleted_files']} data files"
        )


if __name__ == "__main__":
    main()
