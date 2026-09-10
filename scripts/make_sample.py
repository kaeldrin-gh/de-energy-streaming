"""Refresh ``data/sample/latest.json`` from the live SMARD API.

Run this when you want to refresh the bundled offline sample:

    python scripts/make_sample.py --hours 168

The file is committed so the repo can demo the whole pipeline without
network access.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

# Allow `python scripts/make_sample.py` from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer.config import Settings  # noqa: E402
from producer.smard import SmardClient  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=168, help="hours of history to keep")
    args = parser.parse_args()

    settings = Settings()
    client = SmardClient(settings.smard_base_url, settings.smard_filter, settings.smard_region)

    now = dt.datetime.now(dt.UTC)
    points = client.fetch_latest(weeks=2)
    points = [point for point in points if point.delivery_ts_utc <= now][-args.hours :]
    if not points:
        raise SystemExit("SMARD returned no published points; nothing written")

    payload = {
        "meta": {
            "source": "SMARD.de (Bundesnetzagentur) - Marktpreis DE-LU",
            "filter": settings.smard_filter,
            "region": settings.smard_region,
            "resolution": "hour",
            "generated_at": now.isoformat(),
            "hours": len(points),
        },
        "series": [
            [int(point.delivery_ts_utc.timestamp() * 1000), point.price_eur_mwh] for point in points
        ],
    }

    out_dir = Path(settings.sample_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "latest.json"
    out_path.write_text(json.dumps(payload, indent=1), encoding="utf-8")

    first = points[0].delivery_ts_utc.isoformat()
    last = points[-1].delivery_ts_utc.isoformat()
    print(f"wrote {len(points)} points ({first} .. {last}) -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
