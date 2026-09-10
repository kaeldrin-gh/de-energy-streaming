# ADR 0001: Local-first, zero-cost stack

- **Status:** accepted
- **Date:** 2026-09-10

## Context

This repository exists to demonstrate production-style data engineering to
reviewers who will clone it and try to run it. Two constraints shaped the
architecture:

1. **A reviewer must be able to run everything without an account, a credit
   card, or a cloud console.** Free tiers expire, require signups, and break
   when quotas change; a portfolio project whose demo breaks next quarter is
   worse than no demo.
2. **The skills demonstrated must be the real ones.** A toy in-process
   pipeline would be runnable but would not demonstrate Kafka, Spark,
   Iceberg, orchestration, or infrastructure-as-code.

## Decision

Run the whole platform with open-source containers via `docker compose`:

- Kafka API: Redpanda (single binary, Kafka-compatible)
- Lakehouse storage: LocalStack S3 (real S3 API, runs locally)
- Table format: Apache Iceberg on S3A
- Processing: Spark 3.5 (Structured Streaming + batch) on a standalone cluster
- Orchestration: Airflow (LocalExecutor)
- Serving: PostgreSQL + Grafana
- Infrastructure: Terraform against LocalStack

External data comes from SMARD.de (Bundesnetzagentur), whose chart endpoints
need **no key and no registration**. The bundled `data/sample/latest.json` is
real captured data so the pipeline also runs fully offline.

## Consequences

- The only prerequisite is Docker (Desktop, free) and network access for the
  first image pull. CI validates the compose file, Terraform, and the unit
  tests without needing any cloud credentials.
- No secrets are stored in the repository; the LocalStack credentials are the
  well-known `test`/`test` pair and are documented as such.
- The same Spark/Iceberg code targets real AWS S3 by changing
  `S3_ENDPOINT`, `ICEBERG_WAREHOUSE`, and credentials - the S3A Hadoop
  connector is the single integration point.
- Free managed services (e.g. a hosted Kafka or Postgres) can be substituted
  per-component through environment variables, but are deliberately not the
  default: a clone-and-run experience must not depend on anyone's account.
