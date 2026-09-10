"""Kafka publishing.

The Kafka client is imported lazily so tests and ``--dry-run`` work without
``confluent-kafka`` or a running broker.
"""

from __future__ import annotations

import json
import logging

from producer.config import Settings
from producer.smard import PricePoint

log = logging.getLogger(__name__)


class KafkaSink:
    def __init__(self, settings: Settings) -> None:
        from confluent_kafka import Producer  # lazy: optional at import time

        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "smard-producer",
                "enable.idempotence": True,
            }
        )

    @staticmethod
    def key_for(point: PricePoint) -> str:
        """Natural key: one Kafka key per (region, delivery hour).

        Downstream MERGE semantics make re-publishing the same hour safe;
        the key keeps revisions of an hour on one partition, in order.
        """
        return f"{point.region}|{point.delivery_ts_utc.isoformat()}"

    def publish(self, point: PricePoint) -> None:
        payload = json.dumps(point.to_message(), separators=(",", ":")).encode()
        self._producer.produce(
            self._settings.topic_prices,
            key=self.key_for(point).encode(),
            value=payload,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        remaining = self._producer.flush(timeout)
        if remaining:
            log.warning("%d messages still in the producer queue after flush", remaining)
        return remaining
