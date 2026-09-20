"""Markdown market pulse for the CI run page.

Fetches the latest published SMARD.de day-ahead prices through the producer's
client (no API key, no Docker, no Spark) and renders a compact metrics table -
the de-energy-streaming analogue of the ingest summary in the batch project.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from zoneinfo import ZoneInfo

from producer.config import Settings
from producer.news import ClassifiedHeadline, classify, default_feeds, fetch_feeds
from producer.smard import PricePoint, SmardClient

log = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Europe/Berlin")
UTC = dt.timezone.utc
WINDOWS = (1, 7, 14)


def _window(points: list[PricePoint], latest: dt.datetime, days: int) -> list[PricePoint]:
    start = latest - dt.timedelta(days=days)
    return [point for point in points if point.delivery_ts_utc > start]


def _stats(rows: list[PricePoint]) -> dict | None:
    prices = [point.price_eur_mwh for point in rows]
    if not prices:
        return None
    return {
        "hours": len(prices),
        "avg": sum(prices) / len(prices),
        "min": min(prices),
        "max": max(prices),
        "negative": sum(1 for price in prices if price < 0),
    }


def _mean(rows: list[PricePoint]) -> float | None:
    return sum(point.price_eur_mwh for point in rows) / len(rows) if rows else None


def _fmt(value: float | None, digits: int = 2) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _local(when: dt.datetime) -> str:
    return when.astimezone(LOCAL_TZ).strftime("%a %d %b %H:%M")


def _news_section(news: list[ClassifiedHeadline]) -> list[str]:
    """Markdown rows for the latest energy topics plus a filtered-out note."""
    counts: dict[str, int] = {}
    for item in news:
        if item.category:
            counts[item.category] = counts.get(item.category, 0) + 1
    filtered = len(news) - sum(counts.values())
    lines = [
        "",
        f"| Energy topic (latest {len(news)} headlines) | Headlines |",
        "| --- | ---: |",
    ]
    if counts:
        lines += [
            f"| {topic} | {count} |"
            for topic, count in sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
        ]
    else:
        lines.append("| (none found) | 0 |")
    lines += [
        "",
        f"_{filtered} of {len(news)} headlines were general news and filtered out._",
    ]
    return lines


def render_summary(
    points: list[PricePoint],
    now: dt.datetime | None = None,
    news: list[ClassifiedHeadline] | None = None,
) -> str:
    """Render the market pulse as Markdown for GITHUB_STEP_SUMMARY."""
    now = now or dt.datetime.now(UTC)
    published = sorted(
        (point for point in points if point.delivery_ts_utc <= now),
        key=lambda point: point.delivery_ts_utc,
    )
    if not published:
        return (
            "## de-energy-streaming - German day-ahead market pulse\n\n"
            "SMARD returned no published hours.\n"
        )

    latest = published[-1].delivery_ts_utc
    lines = [
        "## de-energy-streaming - German day-ahead market pulse",
        "",
        f"SMARD.de (Bundesnetzagentur), DE-LU day-ahead, published hours only "
        f"(latest {latest:%Y-%m-%d %H:%M} UTC) - rendered {now:%Y-%m-%d %H:%M} UTC",
        "",
        "| Window | Avg EUR/MWh | Min | Max | Negative hours |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for days in WINDOWS:
        stats = _stats(_window(published, latest, days))
        if stats is None:
            continue
        share = 100 * stats["negative"] / stats["hours"]
        label = "Last 24 h" if days == 1 else f"Last {days} days"
        lines.append(
            f"| {label} | {_fmt(stats['avg'])} | {_fmt(stats['min'])} | {_fmt(stats['max'])} | "
            f"{stats['negative']} ({share:.1f} %) |"
        )

    fortnight = _window(published, latest, 14)
    cheapest = min(fortnight, key=lambda point: point.price_eur_mwh)
    priciest = max(fortnight, key=lambda point: point.price_eur_mwh)
    weekday = [
        point for point in fortnight if point.delivery_ts_utc.astimezone(LOCAL_TZ).weekday() < 5
    ]
    weekend = [
        point for point in fortnight if point.delivery_ts_utc.astimezone(LOCAL_TZ).weekday() >= 5
    ]

    lines += [
        "",
        "| Extremes (last 14 days) | When (Europe/Berlin) | Price |",
        "| --- | --- | ---: |",
        f"| Cheapest | {_local(cheapest.delivery_ts_utc)} | {_fmt(cheapest.price_eur_mwh)} |",
        f"| Priciest | {_local(priciest.delivery_ts_utc)} | {_fmt(priciest.price_eur_mwh)} |",
        "",
        "| Split (last 14 days) | Avg EUR/MWh | Hours |",
        "| --- | ---: | ---: |",
        f"| Weekdays | {_fmt(_mean(weekday))} | {len(weekday)} |",
        f"| Weekends | {_fmt(_mean(weekend))} | {len(weekend)} |",
    ]
    if news:
        lines += _news_section(news)
    lines += [
        "",
        "Full analysis and charts: `analysis/findings.md`, `make bi`, `make charts`.",
    ]
    return "\n".join(lines)


def _fetch_news() -> list[ClassifiedHeadline] | None:
    """Latest headlines with topics; the price pulse must render regardless."""
    try:
        headlines = fetch_feeds(default_feeds())
        return classify(headlines) if headlines else None
    except Exception as error:  # noqa: BLE001 - news must never break the pulse
        log.warning("news context unavailable: %s", error)
        return None


def run(weeks: int = 3, with_news: bool = True) -> None:
    settings = Settings()
    client = SmardClient(settings.smard_base_url, settings.smard_filter, settings.smard_region)
    points = client.fetch_latest(weeks=weeks)
    news = _fetch_news() if with_news else None
    print(render_summary(points, news=news))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weeks", type=int, default=3, help="weekly SMARD chunks to fetch")
    parser.add_argument("--no-news", action="store_true", help="skip the news context section")
    args = parser.parse_args(argv)
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
    run(weeks=args.weeks, with_news=not args.no_news)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
