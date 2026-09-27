# Operations runbook

Practical commands for a running stack. Assumes `make up` has been executed and
the stack is healthy. For how the streaming path handles retries, duplicates,
ordering and the DLQ, see [event-driven-patterns.md](event-driven-patterns.md).

## Day-to-day

| What | Command |
| --- | --- |
| Start core stack | `make up` |
| Stop (keep data) | `make down` |
| Reset everything | `make clean` (deletes volumes) |
| Service status | `make ps` |
| Follow logs | `make logs` |
| Live SMARD ingest | `make live` (foreground; polls every 60 s) |
| Offline demo replay | `make demo` |
| Streaming job | `make stream` (foreground) |
| Batch backfill | `make backfill` |
| News enrichment (optional) | `make news` |
| Iceberg maintenance | `make maintain` (also daily at 04:30 via `energy_table_maintenance`) |
| Observability stack | `make obs` |

## Inspecting the platform

```bash
# Kafka topics and consumer lag
docker compose exec redpanda rpk topic list --brokers redpanda:9092
docker compose exec redpanda rpk topic consume energy.prices.raw --brokers redpanda:9092 --num 5

# Object storage (LocalStack S3)
docker compose exec localstack awslocal s3 ls s3://energy-lake/warehouse --recursive | head

# Iceberg tables via an interactive Spark SQL shell (local mode inside the container)
docker compose run --rm --entrypoint /opt/spark/bin/spark-sql spark-master \
  --master local[2] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=jdbc \
  --conf spark.sql.catalog.lake.uri=jdbc:postgresql://postgres:5432/iceberg \
  --conf spark.sql.catalog.lake.jdbc.user=energy \
  --conf spark.sql.catalog.lake.jdbc.password=energy \
  --conf spark.sql.catalog.lake.warehouse=s3a://energy-lake/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
  --conf spark.hadoop.fs.s3a.access.key=test \
  --conf spark.hadoop.fs.s3a.secret.key=test \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  -e "SELECT count(*) AS hours, max(delivery_ts) AS latest FROM lake.energy.bronze_prices"

# Serving tables
docker compose exec postgres psql -U energy -d serving \
  -c "SELECT max(delivery_ts), count(*) FROM serving.price_hourly"

# Pipeline health
docker compose exec postgres psql -U energy -d serving \
  -c "SELECT * FROM serving.pipeline_health"
```

Web UIs (all local, default credentials):

| Service | URL | Notes |
| --- | --- | --- |
| Airflow | http://localhost:8088 | `admin` / `admin` |
| Grafana | http://localhost:3000 | `admin` / `admin` (`make obs`) |
| Prometheus | http://localhost:9090 | `make obs` |
| Spark master | http://localhost:8080 | cluster + apps |
| Spark worker | http://localhost:8081 | |

The Spark master UI after a few days of history (the long-running streaming
application plus the batch jobs submitted by Airflow):

![Spark master](images/spark-master.png)

## Failure modes and first checks

| Symptom | First check | Cause / fix |
| --- | --- | --- |
| `make stream` exits with "NoSuchBucket" | `docker compose logs localstack` | LocalStack volume was reset. `docker compose up -d` recreates the bucket via `localstack-init`; or run `make infra` |
| A job logs "files missing from storage, catalog entry dropped" | job output | LocalStack lost the table files but the Postgres catalog kept the entries. Every job heals this on start (`drop_tables_without_storage` in `spark/jobs/common.py`): the empty tables are recreated and `make stream` re-reads Kafka from the start (the checkpoint was lost too). `make backfill` + a transform rebuild the history beyond Kafka's retention |
| `make stream` exits with Iceberg "NotFoundException" for a metadata file | `docker compose exec postgres psql -U energy -d iceberg -c "SELECT * FROM iceberg_tables;"` | Partial loss: some of a table's files survived, so it is not dropped automatically. Clear the entries (`DELETE FROM iceberg_tables WHERE table_namespace='energy';`) and restart the stream; `make backfill` + a transform rebuild the data |
| First Airflow Spark task slow | task log | Ivy resolves ~100 MB of connector jars once, then cached in the `ivy-cache` volume |
| DLQ topic growing | consume `energy.prices.invalid` | Producer schema drift; inspect messages and update `MESSAGE_SCHEMA` in `spark/jobs/stream_prices.py` |
| Serving table empty | `serve` after backfill | Batch/stream hasn't run yet; trigger `energy_batch_pipeline` in Airflow or `make backfill` |
| Health DAG failing | `serving.pipeline_health` | Hours that are due (all of today from 05:00 Berlin) are missing: the 03:00 backfill failed, the hourly transform stopped, or SMARD published late. The detail names the newest hour and what was due |
| Port already in use | `netstat -ano \| findstr 8088` (Windows) | Another Postgres/Airflow instance running; stop it or remap ports in `docker-compose.yml` |
| Docker Desktop memory issues | Settings → Resources | Give Docker ≥ 8 GB RAM; Spark worker is capped at 2 GB |
| News job stores NULL categories | `make news` output | classifier.dev unreachable or rate-limited; headlines are kept and the next run reclassifies them (ADR 0003) |

## Table maintenance

Every streaming micro-batch and every hourly MERGE commits a new Iceberg
snapshot and writes small data files. `energy_table_maintenance` runs
`spark/jobs/maintain_tables.py` daily at 04:30 Europe/Berlin (after the 03:00
backfill, between hourly transforms). For each table in `lake.energy` it:

1. runs `rewrite_data_files` (bin-packing small files per partition, with
   partial progress: a file group whose commit conflicts with a concurrent
   streaming write is skipped and compacted on the next run), then
2. runs `expire_snapshots`, removing snapshots older than 7 days while always
   keeping the newest 10, and deleting the data files no remaining snapshot
   references.

Time travel therefore reaches back at least 7 days. Compaction never changes
rows, and `tests/test_table_maintenance.py` checks that. To inspect the effect:

```sql
SELECT count(*) FROM lake.energy.bronze_prices.files;      -- data files
SELECT count(*) FROM lake.energy.bronze_prices.snapshots;  -- snapshots kept
```

The job prints per table how many files it compacted and how many it deleted.
Iceberg 1.8.1 can also log "partial-progress.enabled is true but N rewrite
commits failed" after a rewrite that succeeded (in the tests, N = 9 after a
single successful commit, out of `partial-progress.max-commits` = 10), so read
the printed counts rather than that line. Any other error fails the task, and
Airflow retries it twice.

## Alerting

The pipeline is built so alerts can be wired in without touching code:

- **Airflow** fails `energy_healthcheck` when hours that are already due are
  missing from the serving layer (all of today from 05:00 Berlin time). A
  notifier plugs in through the DAG's `default_args` (`on_failure_callback`)
  or an Airflow provider; none is configured and no credentials are committed.
- **GitHub Actions** opens one issue when the scheduled `market-summary` run
  fails, and skips creating another while an issue is still open.
- **Grafana** plots `serving.pipeline_health`, so a stale pipeline is visible on
  the dashboard before any alert fires.

## Data semantics worth remembering

- **`null` prices in SMARD responses are normal** - they are future hours of the
  current week, not data loss. The parser skips them.
- **Revisions are expected.** Ingestion is an upsert keyed by
  `(region, delivery_ts)` where the newest `fetched_at` wins; replaying data is
  always safe (see `docs/decisions/0002-revision-aware-upserts.md`).
- **Timestamps** are stored as UTC; the Berlin local hour is derived in the
  silver layer, where weekend/negative/peak flags live.
