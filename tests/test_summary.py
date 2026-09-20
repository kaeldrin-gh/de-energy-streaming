"""Market pulse math: windows, negatives, weekend split, empty input."""

import datetime as dt

from producer.news import ClassifiedHeadline, Headline
from producer.smard import PricePoint
from producer.summary import render_news, render_summary

NOW = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)  # Sunday


def point(hours_ago: int, price: float) -> PricePoint:
    return PricePoint(
        region="DE-LU",
        delivery_ts_utc=NOW - dt.timedelta(hours=hours_ago),
        price_eur_mwh=price,
    )


def test_render_summary_windows_and_split():
    md = render_summary([point(3, -10.0), point(2, 20.0), point(1, 30.0)], now=NOW)

    assert "German day-ahead market pulse" in md
    assert "| Last 24 h | 13.33 | -10.00 | 30.00 | 1 (33.3 %) |" in md
    assert "| Cheapest | Sun 13 Sep 11:00 | -10.00 |" in md
    assert "| Priciest | Sun 13 Sep 13:00 | 30.00 |" in md
    assert "| Weekends | 13.33 | 3 |" in md
    assert "| Weekdays | - | 0 |" in md


def test_render_summary_ignores_unpublished_future_hours():
    md = render_summary([point(1, 50.0), point(-2, 999.0)], now=NOW)

    assert "Last 24 h" in md
    assert "999.00" not in md


def test_render_summary_empty_input():
    md = render_summary([], now=NOW)

    assert "no published hours" in md.lower()


def test_render_summary_includes_news_topics():
    news = [
        ClassifiedHeadline(Headline("s", "https://x/1", "t1", None), "policy", 0.9, "m"),
        ClassifiedHeadline(Headline("s", "https://x/2", "t2", None), "policy", 0.8, "m"),
        ClassifiedHeadline(Headline("s", "https://x/3", "t3", None), None, None, "m"),
    ]
    md = render_summary([point(1, 50.0)], now=NOW, news=news)

    assert "### News context" in md
    assert "| Energy topic (latest 3 headlines) | Headlines |" in md
    assert "| policy | 2 |" in md
    assert "1 of 3 headlines were general news and filtered out." in md
    assert "none / unclassified" not in md


def test_render_news_heading_is_optional():
    news = [ClassifiedHeadline(Headline("s", "https://x/1", "t", None), "gas", 0.9, "m")]

    with_heading = render_news(news)
    without = render_news(news, heading=None)

    assert with_heading.startswith("### News context")
    assert "| gas | 1 |" in without
    assert not without.startswith("#")


def test_render_summary_without_news_has_no_news_table():
    md = render_summary([point(1, 50.0)], now=NOW)

    assert "Energy topic" not in md
