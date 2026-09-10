"""CLI entry point: ``python -m producer live|replay``.

Examples
--------
Offline demo against the bundled sample (no broker required with --dry-run):

    python -m producer replay --speed 50 --dry-run

Live polling of SMARD.de into Kafka (no API key required):

    python -m producer live --interval 60
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from producer.config import Settings
from producer.replay import iter_replay, load_sample
from producer.smard import PricePoint, SmardClient

log = logging.getLogger("producer")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="producer",
        description="SMARD.de (Bundesnetzagentur) day-ahead prices -> Kafka. No API key required.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="print messages instead of publishing"
    )
    parser.add_argument("--log-level", default="INFO")

    sub = parser.add_subparsers(dest="command", required=True)

    live = sub.add_parser("live", help="poll the live SMARD API and publish new/changed hours")
    live.add_argument("--interval", type=float, default=60.0, help="seconds between polls")
    live.add_argument("--weeks", type=int, default=2, help="how many recent weekly chunks to check")
    live.add_argument("--once", action="store_true", help="fetch once and exit")

    replay = sub.add_parser("replay", help="replay the bundled sample (offline, deterministic)")
    replay.add_argument("--sample", type=Path, default=None, help="path to a sample JSON file")
    replay.add_argument("--speed", type=float, default=50.0, help="messages per second")
    return parser


def _emit(sink, point: PricePoint, dry_run: bool) -> None:
    if dry_run:
        print(json.dumps(point.to_message(), separators=(",", ":")))
    else:
        sink.publish(point)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = Settings()
    sink = None
    if not args.dry_run:
        from producer.sink import KafkaSink

        sink = KafkaSink(settings)

    if args.command == "replay":
        sample = args.sample or (Path(settings.sample_dir) / "latest.json")
        points = list(iter_replay(load_sample(sample)))
        log.info("replaying %d points from %s at %.1f msg/s", len(points), sample, args.speed)
        delay = 1.0 / args.speed if args.speed > 0 else 0.0
        for point in points:
            _emit(sink, point, args.dry_run)
            if delay:
                time.sleep(delay)
        if sink:
            sink.flush()
        return 0

    # live mode: emit hours that are new or whose published price changed
    client = SmardClient(settings.smard_base_url, settings.smard_filter, settings.smard_region)
    published: dict[str, float] = {}
    while True:
        points = client.fetch_latest(weeks=args.weeks)
        emitted = 0
        for point in points:
            key = f"{point.region}|{point.delivery_ts_utc.isoformat()}"
            if published.get(key) != point.price_eur_mwh:
                _emit(sink, point, args.dry_run)
                published[key] = point.price_eur_mwh
                emitted += 1
        log.info("live: %d hours checked, %d emitted", len(points), emitted)
        if sink:
            sink.flush()
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
