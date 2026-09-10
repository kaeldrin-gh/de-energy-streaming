"""Tests for the offline replay path and the committed real-data sample."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from producer.replay import iter_replay, load_sample
from producer.sink import KafkaSink

SAMPLE = Path("data/sample/latest.json")


@pytest.fixture(scope="module")
def points():
    if not SAMPLE.exists():
        pytest.skip("sample missing - run `python scripts/make_sample.py`")
    return load_sample(SAMPLE)


def test_sample_is_current_and_well_formed(points):
    assert len(points) >= 48
    assert all(point.delivery_ts_utc.tzinfo == dt.UTC for point in points)
    assert all(point.region == "DE-LU" for point in points)
    assert all(isinstance(point.price_eur_mwh, float) for point in points)


def test_replay_is_deterministic(points):
    first = list(iter_replay(points))
    second = list(iter_replay(reversed(points)))
    assert first == second
    assert first == sorted(first, key=lambda point: (point.delivery_ts_utc, point.region))


def test_message_keys_are_one_per_delivery_hour(points):
    keys = {KafkaSink.key_for(point) for point in points}
    assert len(keys) == len(points)


def test_sample_meta_documents_provenance():
    payload = json.loads(SAMPLE.read_text(encoding="utf-8"))
    assert "SMARD" in payload["meta"]["source"]
    assert payload["meta"]["filter"] == "4169"
    assert payload["meta"]["region"] == "DE-LU"
    assert payload["meta"]["resolution"] == "hour"
