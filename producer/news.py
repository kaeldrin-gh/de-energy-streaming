"""Energy-news headlines with topic classification.

Fetches public RSS/Atom feeds and classifies every headline through
classifier.dev (keyless, free tier). This is an *optional enrichment*: if the
classifier is unreachable, headlines are still ingested with a NULL category and
a later run reclassifies them (newer ``fetched_at`` wins, same rule as prices).
"""

from __future__ import annotations

import datetime as dt
import email.utils
import html
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from urllib.parse import urlparse

import requests

log = logging.getLogger(__name__)

DEFAULT_FEEDS = [
    "https://www.pv-magazine.de/feed/",
    "https://www.cleanenergywire.org/rss.xml",
    "https://www.solarserver.de/feed/",
]

CLASSIFIER_LABELS = [
    "grid and infrastructure",
    "policy and regulation",
    "power prices and markets",
    "gas",
    "renewables",
    "batteries and storage",
    "hydrogen",
    "companies and projects",
    "weather",
    "none of these",
]

CLASSIFIER_INSTRUCTIONS = (
    "Classify German, Dutch and European energy news. If a headline touches any "
    "part of the energy system - generation, grids, power markets, prices, "
    "policy, storage, hydrogen, gas, utilities or projects - pick the closest "
    "topic. Use 'none of these' only for clearly unrelated news (general "
    "finance, macro, housing, labour, sport, culture)."
)

USER_AGENT = "de-energy-streaming/0.1 (+https://github.com/kaeldrin-gh/de-energy-streaming)"
ATOM = "{http://www.w3.org/2005/Atom}"
_TAG_RE = re.compile(r"<[^>]+>")
_RETRYABLE = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Headline:
    source: str
    link: str
    title: str
    published_ts: dt.datetime | None

    @property
    def key(self) -> str:
        return f"{self.source}|{self.link}"


@dataclass(frozen=True)
class ClassifiedHeadline:
    headline: Headline
    category: str | None
    confidence: float | None
    model: str | None


def default_feeds() -> list[str]:
    """Feed list from NEWS_FEEDS (comma-separated) or the built-in defaults."""
    override = os.environ.get("NEWS_FEEDS", "").strip()
    if override:
        return [url.strip() for url in override.split(",") if url.strip()]
    return list(DEFAULT_FEEDS)


def source_name(url: str) -> str:
    return urlparse(url).netloc or url


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", html.unescape(_TAG_RE.sub("", text))).strip()


def _to_utc(value: dt.datetime | None) -> dt.datetime | None:
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(dt.timezone.utc).replace(tzinfo=None)


def _parse_date(text: str | None) -> dt.datetime | None:
    if not text:
        return None
    text = text.strip()
    try:
        return _to_utc(email.utils.parsedate_to_datetime(text))
    except (TypeError, ValueError):
        pass
    try:
        return _to_utc(dt.datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def parse_feed(content: bytes, source: str) -> list[Headline]:
    """Parse RSS 2.0 or Atom entries; items without title/link are skipped."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError as error:
        raise ValueError(f"feed is not valid XML: {error}") from error

    items = root.findall(".//item") or root.findall(f".//{ATOM}entry")
    headlines: list[Headline] = []
    seen: set[str] = set()
    for item in items:
        title = _clean(item.findtext("title") or item.findtext(f"{ATOM}title"))
        link = ""
        atom_link = item.find(f"{ATOM}link")
        if atom_link is not None:
            link = (atom_link.get("href") or "").strip()
        if not link:
            link = _clean(item.findtext("link"))
        if not title or not link or link in seen:
            continue
        seen.add(link)
        published = (
            item.findtext("pubDate")
            or item.findtext(f"{ATOM}updated")
            or item.findtext(f"{ATOM}published")
        )
        headlines.append(
            Headline(
                source=source,
                link=link,
                title=title,
                published_ts=_parse_date(published),
            )
        )
    return headlines


def fetch_feed(url: str, timeout: int = 30) -> list[Headline]:
    response = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return parse_feed(response.content, source=source_name(url))


def fetch_feeds(feeds: list[str], timeout: int = 30) -> list[Headline]:
    """Fetch every feed; a broken feed is logged and skipped (source isolation)."""
    headlines: list[Headline] = []
    for url in feeds:
        try:
            headlines.extend(fetch_feed(url, timeout=timeout))
        except (requests.RequestException, ValueError) as error:
            log.warning("feed %s failed (%s) - skipping", url, error)
    return headlines


def classifier_url() -> str:
    return os.environ.get("CLASSIFIER_URL", "https://classifier.dev").rstrip("/")


def _post_classifier(body: dict, timeout: int, attempts: int = 3) -> dict:
    """POST to the classifier with a small retry ladder for 429/5xx."""
    delay = 5.0
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            response = requests.post(
                classifier_url(),
                json=body,
                timeout=timeout,
                headers={"User-Agent": USER_AGENT},
            )
            if response.status_code in _RETRYABLE:
                raise requests.HTTPError(
                    f"HTTP {response.status_code} from classifier", response=response
                )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            last_error = error
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"classifier unavailable: {last_error}")


def classify(headlines: list[Headline], timeout: int = 60) -> list[ClassifiedHeadline]:
    """Assign one topic per headline; on failure headlines stay unclassified."""
    if not headlines:
        return []
    body = {
        "labels": CLASSIFIER_LABELS,
        "inputs": [headline.title for headline in headlines],
        "instructions": CLASSIFIER_INSTRUCTIONS,
    }
    try:
        payload = _post_classifier(body, timeout=timeout)
    except RuntimeError as error:
        log.warning("%s - storing %d headlines unclassified", error, len(headlines))
        return [ClassifiedHeadline(headline, None, None, None) for headline in headlines]

    results = payload.get("results", [])
    classified: list[ClassifiedHeadline] = []
    for index, headline in enumerate(headlines):
        result = results[index] if index < len(results) else {}
        label = result.get("label")
        confidence = result.get("confidence")
        model = result.get("model")
        if label in (None, "none of these"):
            classified.append(ClassifiedHeadline(headline, None, None, model))
        else:
            classified.append(
                ClassifiedHeadline(
                    headline,
                    label,
                    float(confidence) if confidence is not None else None,
                    model,
                )
            )
    return classified


def run_news(feeds: list[str] | None = None, limit: int = 0) -> None:
    """CLI preview: fetch, classify and print one JSON line per headline."""
    headlines = fetch_feeds(feeds or default_feeds())
    if limit:
        headlines = headlines[:limit]
    classified = classify(headlines)
    for item in classified:
        print(
            json.dumps(
                {
                    "source": item.headline.source,
                    "link": item.headline.link,
                    "title": item.headline.title,
                    "published_ts": (
                        item.headline.published_ts.isoformat()
                        if item.headline.published_ts
                        else None
                    ),
                    "category": item.category,
                    "confidence": item.confidence,
                    "model": item.model,
                },
                ensure_ascii=False,
            )
        )
    kept = sum(1 for item in classified if item.category)
    print(f"# {len(classified)} headlines, {kept} classified, {len(classified) - kept} other")
