"""Parser and classifier tests for the news enrichment (no network)."""

import datetime as dt

from producer import news

RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Netzbetreiber meldet St&#246;rung in Bayern</title>
    <link>https://example.de/a</link>
    <pubDate>Sat, 19 Sep 2026 08:00:00 +0000</pubDate>
  </item>
  <item>
    <title><![CDATA[Neues Windgesetz <b>beschlossen</b>]]></title>
    <link>https://example.de/b</link>
    <pubDate>2026-09-19T09:00:00Z</pubDate>
  </item>
  <item><title>Ohne Link</title></item>
</channel></rss>"""

ATOM = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Batteriespeicher in Betrieb genommen</title>
    <link href="https://example.org/x"/>
    <updated>2026-09-18T10:00:00Z</updated>
  </entry>
</feed>"""


def test_parse_rss_strips_markup_and_entities():
    items = news.parse_feed(RSS, source="example.de")

    assert [item.title for item in items] == [
        "Netzbetreiber meldet Störung in Bayern",
        "Neues Windgesetz beschlossen",
    ]
    assert items[0].link == "https://example.de/a"
    assert items[0].published_ts == dt.datetime(2026, 9, 19, 8, 0)
    assert items[1].published_ts == dt.datetime(2026, 9, 19, 9, 0)
    assert items[1].key == "example.de|https://example.de/b"


def test_parse_atom_uses_link_href():
    items = news.parse_feed(ATOM, source="example.org")

    assert len(items) == 1
    assert items[0].link == "https://example.org/x"
    assert items[0].published_ts == dt.datetime(2026, 9, 18, 10, 0)


def test_fetch_feeds_isolates_broken_feeds(monkeypatch):
    def fake_fetch(url: str, timeout: int = 30):
        if "broken" in url:
            raise ValueError("not valid XML")
        return [news.Headline("good.example", "https://good.example/1", "Titel", None)]

    monkeypatch.setattr(news, "fetch_feed", fake_fetch)
    items = news.fetch_feeds(["https://good.example/rss", "https://broken.example/rss"])

    assert len(items) == 1
    assert items[0].key == "good.example|https://good.example/1"


def test_classify_maps_labels_and_treats_none_as_unclassified(monkeypatch):
    def fake_post(body, timeout, attempts=3):
        assert body["labels"][-1] == "none of these"
        return {
            "results": [
                {"label": "grid and infrastructure", "confidence": 0.95, "model": "m1"},
                {"label": "none of these", "confidence": 0.8, "model": "m1"},
            ]
        }

    monkeypatch.setattr(news, "_post_classifier", fake_post)
    headlines = [
        news.Headline("s", "https://x/1", "Störung im Netz", None),
        news.Headline("s", "https://x/2", "Wetterbericht", None),
    ]
    classified = news.classify(headlines)

    assert classified[0].category == "grid and infrastructure"
    assert classified[0].confidence == 0.95
    assert classified[1].category is None
    assert classified[1].confidence is None
    assert classified[1].model == "m1"


def test_classify_keeps_headlines_when_the_classifier_is_down(monkeypatch):
    def boom(body, timeout, attempts=3):
        raise RuntimeError("classifier unavailable")

    monkeypatch.setattr(news, "_post_classifier", boom)
    classified = news.classify([news.Headline("s", "https://x/1", "Titel", None)])

    assert len(classified) == 1
    assert classified[0].category is None
    assert classified[0].headline.title == "Titel"


def test_classify_empty_input():
    assert news.classify([]) == []
