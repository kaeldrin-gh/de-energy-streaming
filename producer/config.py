"""Environment-driven settings. Defaults match docker-compose.yml."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_prices: str = field(
        default_factory=lambda: _env("KAFKA_TOPIC_PRICES", "energy.prices.raw")
    )
    topic_dlq: str = field(default_factory=lambda: _env("KAFKA_TOPIC_DLQ", "energy.prices.invalid"))

    # SMARD.de public API (no key required)
    smard_base_url: str = field(
        default_factory=lambda: _env("SMARD_BASE_URL", "https://www.smard.de/app/chart_data")
    )
    smard_filter: str = field(default_factory=lambda: _env("SMARD_FILTER", "4169"))
    smard_region: str = field(default_factory=lambda: _env("SMARD_REGION", "DE-LU"))

    # Bundled offline sample
    sample_dir: str = field(default_factory=lambda: _env("SAMPLE_DIR", "data/sample"))
