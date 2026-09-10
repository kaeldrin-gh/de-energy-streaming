"""Unit tests for the SMARD client: URL shape, parsing, null handling.

No network access required - payloads are inline fixtures shaped like the
real responses (verified against the live service).
"""

from __future__ import annotations

import datetime as dt

import pytest

from producer.smard import PricePoint, SmardClient, SmardError, parse_chunk

INDEX_PAYLOAD = {"timestamps": [1700000000000, 1700604800000]}

CHUNK_PAYLOAD = {
    "meta_data": {"version": 1, "created": 1789041440260},
    "series": [
        [1788732000000, 155.5],
        [1788735600000, -12.4],
        [1788739200000, None],  # unpublished future hour
        [1788742800000, 148.21],
        ["not-a-list", 1.0],  # malformed row -> skipped
        [1788746400000, "bad"],  # unparsable price -> skipped
    ],
}


def test_urls_match_documented_endpoint_shape():
    client = SmardClient("https://www.smard.de/app/chart_data", "4169", "DE-LU")
    assert client.index_url() == "https://www.smard.de/app/chart_data/4169/DE-LU/index_hour.json"
    assert (
        client.chunk_url(1788732000000)
        == "https://www.smard.de/app/chart_data/4169/DE-LU/4169_DE-LU_hour_1788732000000.json"
    )


def test_parse_chunk_skips_nulls_and_malformed_rows_and_sorts():
    points = parse_chunk(CHUNK_PAYLOAD, region="DE-LU")
    assert [point.price_eur_mwh for point in points] == [155.5, -12.4, 148.21]
    assert all(point.delivery_ts_utc.tzinfo == dt.UTC for point in points)
    assert points == sorted(points, key=lambda point: point.delivery_ts_utc)


def test_parse_chunk_rejects_missing_series():
    with pytest.raises(SmardError):
        parse_chunk({"meta_data": {}}, region="DE-LU")


def test_price_point_message_is_json_friendly():
    point = PricePoint(
        region="DE-LU",
        delivery_ts_utc=dt.datetime(2026, 9, 7, tzinfo=dt.UTC),
        price_eur_mwh=-12.4,
    )
    message = point.to_message()
    assert message["region"] == "DE-LU"
    assert message["price_eur_mwh"] == -12.4
    assert message["delivery_ts"].startswith("2026-09-07T00:00:00")
    # fetched_at must round-trip through ISO parsing
    assert dt.datetime.fromisoformat(message["fetched_at"])


def test_fetch_latest_combines_index_and_chunks():
    client = SmardClient("https://example.invalid", "4169", "DE-LU")
    calls: list[str] = []

    def fake_get_json(url: str) -> dict:
        calls.append(url)
        if url.endswith("index_hour.json"):
            return INDEX_PAYLOAD
        # one point per chunk, timestamped with the week-start from the URL
        week_start = int(url.rsplit("_", 1)[-1].split(".")[0])
        return {"meta_data": {}, "series": [[week_start, 42.0]]}

    client._get_json = fake_get_json  # type: ignore[method-assign]

    points = client.fetch_latest(weeks=2)

    assert len(points) == 2
    assert calls[0].endswith("index_hour.json")
    assert calls[1].rsplit("/", 1)[-1] == "4169_DE-LU_hour_1700000000000.json"
    assert calls[2].rsplit("/", 1)[-1] == "4169_DE-LU_hour_1700604800000.json"


def test_fetch_latest_errors_on_empty_index():
    client = SmardClient("https://example.invalid", "4169", "DE-LU")
    client._get_json = lambda url: {"timestamps": []}  # type: ignore[method-assign]
    with pytest.raises(SmardError):
        client.fetch_latest(weeks=1)


@pytest.mark.integration
def test_live_smard_has_current_data():
    client = SmardClient("https://www.smard.de/app/chart_data", "4169", "DE-LU")
    points = client.fetch_latest(weeks=1)
    assert points, "SMARD returned no published points"
    assert any(point.price_eur_mwh < 0 for point in points) or points[0].price_eur_mwh > 0
