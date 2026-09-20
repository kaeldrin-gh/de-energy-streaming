"""Batch: public energy-news feeds -> classified headlines -> Iceberg.

Run via Airflow (``energy_news_ingest``) or manually:

    spark-submit --master spark://spark-master:7077 /opt/jobs/news_ingest.py

Classification is an optional enrichment (ADR 0003): when the classifier is
unreachable the headline is still stored with a NULL category, and the next run
reclassifies it through the same revision-aware MERGE as everything else.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common import build_spark, ensure_tables, merge_news_from_view

from producer.news import classify, default_feeds, fetch_feeds

UTC = timezone.utc

NEWS_SCHEMA = (
    "source string, link string, title string, published_ts timestamp, "
    "category string, confidence double, model string, fetched_at timestamp"
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--feeds",
        default=None,
        help="comma-separated feed URLs (default: NEWS_FEEDS env or the built-in list)",
    )
    parser.add_argument("--limit", type=int, default=0, help="max headlines to ingest (0 = all)")
    args, _ = parser.parse_known_args()

    spark = build_spark("news_ingest")
    ensure_tables(spark)

    if args.feeds:
        feeds = [url.strip() for url in args.feeds.split(",") if url.strip()]
    else:
        feeds = default_feeds()

    headlines = fetch_feeds(feeds)
    if args.limit:
        headlines = headlines[: args.limit]
    if not headlines:
        raise SystemExit("no headlines fetched; nothing to ingest")

    classified = classify(headlines)
    fetched_at = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (
            item.headline.source,
            item.headline.link,
            item.headline.title,
            item.headline.published_ts,
            item.category,
            item.confidence,
            item.model,
            fetched_at,
        )
        for item in classified
    ]

    frame = spark.createDataFrame(rows, NEWS_SCHEMA)
    frame.createOrReplaceTempView("news_batch")
    merge_news_from_view(spark, "news_batch")
    spark.catalog.dropTempView("news_batch")

    kept = sum(1 for row in rows if row[4])
    print(
        f"news ingest complete: {len(rows)} headlines, {kept} classified, {len(rows) - kept} other"
    )


if __name__ == "__main__":
    main()
