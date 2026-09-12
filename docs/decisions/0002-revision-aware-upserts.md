# ADR 0002: Revision-aware upserts instead of append-only ingestion

- **Status:** accepted
- **Date:** 2026-09-10

## Context

SMARD.de republishes weekly files as auctions clear and corrections happen:
hours that were `null` become prices, and previously published values can
change. On top of that, the pipeline itself produces duplicates by design:

- the producer may re-send an hour when its price changes,
- replaying the sample (demos, tests, disaster recovery) re-sends history,
- at-least-once delivery is the only honest guarantee for both Kafka and
  file-based ingestion.

An append-only landing table would therefore accumulate conflicting rows for
the same delivery hour, and every downstream query would need deduplication
logic that is easy to get wrong.

## Decision

Treat `(region, delivery_ts)` as the natural key of a price observation and
make every write a **revision-aware upsert**:

- Kafka messages are keyed by `region|delivery_ts` (revisions of an hour stay
  ordered on one partition).
- The streaming job MERGEs each micro-batch into `bronze_prices`, updating a
  row only when the incoming `fetched_at` is newer.
- The batch backfill uses the identical MERGE semantics, so history can be
  re-ingested at any time without corrupting newer revisions.
- Checkpoints make Kafka offset recovery idempotent on top of that.

The precedence rule (`newer fetched_at wins`) lives in one place - the MERGE
statement - and is exercised by `tests/test_bronze_merge.py`, which replays
revisions against a throwaway Iceberg warehouse.

## Consequences

- Ingestion is safe to retry: replaying the same 168 hours converges to the
  same table contents.
- Late-arriving corrections win naturally, and out-of-order batches cannot
  regress a price to an older revision.
- The table format must support row-level updates; Iceberg does. This is a
  deliberate part of the stack choice, not an accident.
- Per-micro-batch MERGEs cost more than blind appends. At this data volume
  (one row per hour) that is irrelevant; at a million rows per second the
  typical answer is partition-scoped MERGEs and a compaction job - noted as a
  scaling consideration in the repository README.
