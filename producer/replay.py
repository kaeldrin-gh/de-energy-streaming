"""Deterministic replay of the bundled SMARD sample.

``data/sample/latest.json`` is real DE-LU day-ahead prices captured from
SMARD.de by ``scripts/make_sample.py``. Replaying it lets the whole pipeline
run with no network access and makes demos reproducible.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from collections.abc import Iterable, Iterator
from pathlib import Path

from producer.smard import PricePoint

log = logging.getLogger(__name__)


def load_sample(path: Path, region: str | None = None) -> list[PricePoint]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    region = region or payload.get("meta", {}).get("region", "DE-LU")

    points: list[PricePoint] = []
    for ts_ms, price in payload["series"]:
        if price is None:
            continue
        points.append(
            PricePoint(
                region=region,
                delivery_ts_utc=dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.UTC),
                price_eur_mwh=float(price),
            )
        )
    return points


def iter_replay(points: Iterable[PricePoint]) -> Iterator[PricePoint]:
    """Deterministic order: delivery timestamp, then region."""
    return iter(sorted(points, key=lambda point: (point.delivery_ts_utc, point.region)))
