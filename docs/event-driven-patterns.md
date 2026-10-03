# Event-driven patterns

This page shows how the streaming path handles the usual failures of
event-driven systems. It also shows where each pattern is in this repository.

The path is Kafka (Redpanda locally) → Spark Structured Streaming → Iceberg.
A batch backfill and an Airflow health check work around it.

| Concern | Pattern | Where |
| --- | --- | --- |
| Delivery semantics | Kafka delivers at least once. The writes are idempotent, so duplicates do not cause problems | `spark/jobs/stream_prices.py` (module docstring) |
| Order | Each `(region, delivery hour)` has one Kafka key. Thus, all revisions of an hour stay on one partition, in order | `producer/sink.py` (`key_for`) |
| Idempotent upserts | A revision-aware MERGE keeps one row for each natural key. The newest `fetched_at` wins | `spark/jobs/common.py` (`merge_bronze_from_view`), ADR 0002 |
| Producer durability | `enable.idempotence=True`. `flush(timeout=30)` gives a warning when messages are still in the queue | `producer/sink.py` |
| Retries and backoff | The HTTP client tries four more times, with exponential backoff. Then it stops with an error. It does not publish incomplete data | `producer/smard.py` |
| Poison messages | Records that fail the validation go to the DLQ topic. The job never drops them without a record | `spark/jobs/stream_prices.py` (`route_to_dlq`) |
| Replay | A deterministic replay of a stored SMARD sample. It needs no network | `producer/replay.py` |
| Backfill | A batch job writes to the same bronze table with the same MERGE | `spark/jobs/backfill_prices.py` |
| Progress and recovery | Each query (bronze, DLQ) has its own checkpoint. After a restart, the stream continues from the checkpoint. It does not process all data again | `spark/jobs/stream_prices.py` |
| Freshness monitoring | Every 30 minutes, a check makes sure that each due hour is in the serving tables. From 05:00 Berlin time, all hours of today are due. The check writes `serving.pipeline_health`. If hours are missing, the DAG fails | `airflow/dags/energy_healthcheck.py` |

## At-least-once delivery, exactly-once effects

Kafka delivers each message at least once. After a restart, Spark can deliver a
micro-batch again from its checkpoint. This is not a problem, because the write
is idempotent. A replayed batch gives the same table state. These tests show
it:

- `tests/test_bronze_merge.py::test_replaying_the_same_batch_converges`
- `tests/test_bronze_merge.py::test_duplicates_inside_one_batch_keep_the_newest`

## Order by key

`key_for()` gives the key `region|delivery_ts`. Thus, all revisions of an hour
go to the same partition and arrive in the order of publication.

But the consumer does not need this order. The MERGE compares `fetched_at`, so
a revision that arrives late cannot replace a newer one
(`test_older_revision_cannot_overwrite_newer`). Here, the order by key makes
the processing more efficient. It is not necessary for correct data.

## Idempotent upserts

This is the main pattern. Each `(region, delivery_ts)` has one row, and
`fetched_at` selects the row that stays. The MERGE updates a row only when the
new revision is newer. Thus, replays, upstream corrections and duplicate batches
are safe. For the full reasons, refer to
`docs/decisions/0002-revision-aware-upserts.md`.

## Retries and backoff

When a request to SMARD fails, the client tries four more times. Before each
try, it waits 2^attempt seconds. If all tries fail, it raises `SmardError`.
Then the producer stops with an error. It does not publish an incomplete batch.

You can run the producer again at any time without risk. For the same reason,
you can run `make demo` and `producer/replay.py` at any time.

## Dead-letter queue

Some records have no delivery timestamp, no price or a `fetched_at` that the job
cannot parse. The job separates these records before the write. It sends them
to `energy.prices.invalid` (`KAFKA_TOPIC_DLQ`). The job never drops a record
without a record of it.

For the commands that examine the DLQ, refer to the runbook. Its failure table
has the row "The DLQ topic becomes larger".

## Checkpoints, replay and backfill

Each query has its own checkpoint in S3 (LocalStack locally). Thus, the bronze
stream and the DLQ stream can restart independently.

`producer/replay.py` replays the stored sample (`data/sample/latest.json`) in
the same order each time. Use it for demos without network access.
`spark/jobs/backfill_prices.py` loads historical ranges with the same MERGE.

## Monitoring

`energy_healthcheck` runs every 30 minutes. It makes sure that each hour that
is due is in the serving layer.

SMARD publishes the prices of a day on the afternoon before. The 03:00 backfill
loads them, also when no live producer runs. Thus, from 05:00 Berlin time, all
hours of today must be in the serving layer. Before 05:00, all hours of
yesterday must be there.

A limit on the age of the newest hour does not work here. The newest hour is
usually tomorrow evening. With such a limit, a pipeline that stopped can stay
unseen for most of a day.

The check writes its result to `serving.pipeline_health`, and Grafana shows it.
A missing hour makes the task fail. Thus, Airflow alerting works without more
configuration.

## What this does not claim

- The pipeline is not exactly-once from end to end. It delivers at least once,
  and its effects are idempotent.
- The order is for each key. It is not a global order.
- The pipeline has one source and one region. It has no schema registry. The
  stream job checks the schema and sends failed records to the DLQ.
