# Event-driven patterns

How the streaming path handles the failure modes that show up in event-driven
systems, and where each pattern lives in this repository. The path is Kafka
(Redpanda locally) → Spark Structured Streaming → Iceberg, with a batch backfill
and an Airflow healthcheck around it.

| Concern | Pattern | Where |
| --- | --- | --- |
| Delivery semantics | at-least-once from Kafka; writes are idempotent, so duplicates are harmless | `spark/jobs/stream_prices.py` (module docstring) |
| Ordering | one Kafka key per `(region, delivery hour)`, so revisions of an hour stay on one partition, in order | `producer/sink.py` (`key_for`) |
| Idempotent upserts | revision-aware MERGE: newest `fetched_at` wins, one row per natural key | `spark/jobs/common.py` (`merge_bronze_from_view`), ADR 0002 |
| Producer durability | `enable.idempotence=True`; `flush(timeout=30)` warns when messages are still queued | `producer/sink.py` |
| Retries and backoff | HTTP client retries four times with exponential backoff, then raises instead of publishing partial data | `producer/smard.py` |
| Poison messages | records that fail validation go to the DLQ topic, never silently dropped | `spark/jobs/stream_prices.py` (`route_to_dlq`) |
| Replay | deterministic replay of a captured SMARD sample, no network needed | `producer/replay.py` |
| Backfill | batch job writes the same bronze table through the same MERGE | `spark/jobs/backfill_prices.py` |
| Progress and recovery | separate checkpoints per query (bronze, DLQ); a restart resumes instead of reprocessing everything | `spark/jobs/stream_prices.py` |
| Freshness monitoring | every 30 minutes a 3-hour SLA check writes `serving.pipeline_health` and fails the DAG when stale | `airflow/dags/energy_healthcheck.py` |

## At-least-once delivery, exactly-once effects

Kafka guarantees at-least-once, and Spark checkpointing can re-deliver a
micro-batch after a restart. That is acceptable here because the write is
idempotent: replaying a batch converges to the same table state. The tests make
this explicit:

- `tests/test_bronze_merge.py::test_replaying_the_same_batch_converges`
- `tests/test_bronze_merge.py::test_duplicates_inside_one_batch_keep_the_newest`

## Ordering by key

`key_for()` returns `region|delivery_ts`, so every revision of an hour goes to
the same partition and arrives in publish order. The consumer still does not rely
on order: the MERGE compares `fetched_at`, so an out-of-order revision cannot
overwrite a newer one
(`test_older_revision_cannot_overwrite_newer`). Ordering is an optimisation here,
not a correctness requirement.

## Idempotent upserts

The core pattern: one row per `(region, delivery_ts)`, and `fetched_at` decides
the winner. The MERGE only updates when the incoming revision is newer, which
makes replays, upstream corrections and duplicate batches safe. The full
rationale is in `docs/decisions/0002-revision-aware-upserts.md`.

## Retries and backoff

The SMARD client retries a failed request four times with a 2^attempt-second
backoff before raising `SmardError`; the producer then exits with an error
rather than publishing a partial batch. Re-running is safe, which is also why
`make demo` and `producer/replay.py` can be run at any time.

## Dead-letter queue

Records missing a delivery timestamp, a price or a parseable `fetched_at` are
split off before the write and published to `energy.prices.invalid`
(`KAFKA_TOPIC_DLQ`). Nothing is dropped silently; the runbook has the inspection
commands and the "DLQ topic growing" failure row.

## Checkpoints, replay and backfill

Each query checkpoints separately in S3 (LocalStack locally), so the bronze and
DLQ streams restart independently. `producer/replay.py` replays the captured
sample (`data/sample/latest.json`) deterministically for demos without network
access, and `spark/jobs/backfill_prices.py` fills historical ranges through the
same MERGE.

## Monitoring

`energy_healthcheck` runs every 30 minutes, compares `max(delivery_ts)` in the
serving layer against a 3-hour SLA, records the result in
`serving.pipeline_health` (surfaced in Grafana) and fails the task when the
pipeline is stale, so Airflow alerting works without extra wiring.

## What this does not claim

- Not exactly-once end to end; the guarantee is at-least-once delivery with
  idempotent effects.
- Ordering is per key, not global.
- One source and one region; no schema registry (schema checks live in the
  stream job and fail into the DLQ).
