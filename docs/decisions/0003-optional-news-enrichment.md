# ADR 0003: Optional news enrichment via a keyless classifier

- **Status:** accepted
- **Date:** 2026-09-20

## Context

Prices tell you what happened; they do not tell you why. The warehouse holds
numeric history only, so a €460/MWh Thursday evening spike has no qualitative
context anywhere in the repository. News headlines provide that context cheaply,
but they are prose and cannot be joined to price rows without labels.

Constraints (inherited from ADR 0001):

- no paid service, no account, no API key;
- the pipeline must keep working when the enrichment is unavailable;
- the price path's correctness guarantees must not depend on it.

## Decision

Add an optional **news enrichment** as a separate daily batch job
(`spark/jobs/news_ingest.py`, Airflow DAG `energy_news_ingest`):

- fetch public RSS feeds (pv-magazine, Clean Energy Wire, Solarserver) with
  stdlib XML parsing - no new dependency;
- classify each headline through classifier.dev, a keyless HTTP API with a free
  tier, into `grid and infrastructure / policy and regulation / power prices and
  markets / gas / renewables / batteries and storage / hydrogen / companies and
  projects / weather / none of these`;
- store one row per `(source, link)` in `lake.energy.news_events`, using the
  same revision-aware MERGE rule as prices: newer `fetched_at` wins, so a later
  classification updates the category instead of duplicating the row; a
  `none of these` answer is stored as a NULL category.

Failure semantics:

- a broken feed is logged and skipped (per-source isolation, INC-009);
- a classifier outage stores the headline with a NULL category and the next
  run reclassifies it;
- nothing in the price pipeline depends on this job.

## Consequences

- Price days can be joined to the categories of headlines published around
  them, adding a qualitative dimension to the findings.
- One external dependency exists, confined to one endpoint and one job, and it
  is free and keyless; the rest of the stack stays offline-capable.
- The table grows slowly (a few hundred rows a month); no compaction concern.
- Classification quality is the API's, not ours - the stored `model` and
  `confidence` columns keep the provenance, and unrelated news is stored with a
  NULL category instead of polluting the analytics.
- Feed selection is part of the design: energy-specific sources keep the
  filtered share low (about 17% of headlines on the current feed set) while a
  broad economy feed would make "none of these" the majority.
