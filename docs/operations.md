# Operations runbook

This runbook gives the commands for a stack that operates. Before you use it,
run `make up` and make sure that all services are healthy.

For how the streaming path handles retries, duplicates, order and the DLQ,
refer to [event-driven-patterns.md](event-driven-patterns.md).

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

## Examine the platform

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

The web UIs (all local, with the default credentials):

| Service | URL | Notes |
| --- | --- | --- |
| Airflow | http://localhost:8088 | `admin` / `admin` |
| Grafana | http://localhost:3000 | `admin` / `admin` (`make obs`) |
| Prometheus | http://localhost:9090 | `make obs` |
| Spark master | http://localhost:8080 | cluster + apps |
| Spark worker | http://localhost:8081 | |

The Spark master UI after some days of history. It shows the streaming
application, which runs continuously, and the batch jobs from Airflow:

![Spark master](images/spark-master.png)

## Failure modes and first checks

| Symptom | First check | Cause / fix |
| --- | --- | --- |
| `make stream` stops with "NoSuchBucket" | `docker compose logs localstack` | The LocalStack volume is empty. Run `docker compose up -d`: `localstack-init` makes the bucket again. Or run `make infra` |
| A job shows "files missing from storage, catalog entry dropped" | The job output | LocalStack lost the table files, but the Postgres catalog kept the entries. Each job repairs this when it starts (`drop_tables_without_storage` in `spark/jobs/common.py`). It makes the tables again, empty. `make stream` then reads Kafka from the start, because the checkpoint is also lost. To load the history that Kafka no longer keeps, run `make backfill` and then a transform |
| `make stream` stops with Iceberg "NotFoundException" for a metadata file | `docker compose exec postgres psql -U energy -d iceberg -c "SELECT * FROM iceberg_tables;"` | Some files of a table are lost, but not all. Thus, the job does not remove the entry. Delete the entries (`DELETE FROM iceberg_tables WHERE table_namespace='energy';`). Start the stream again. Run `make backfill` and then a transform to load the data again |
| The first Airflow Spark task is slow | The task log | Ivy downloads approximately 100 MB of connector jars one time. The `ivy-cache` volume keeps them for the next runs |
| The DLQ topic becomes larger | Read `energy.prices.invalid` | The producer schema changed. Examine the messages. Then update `MESSAGE_SCHEMA` in `spark/jobs/stream_prices.py` |
| A serving table is empty | `serve` after a backfill | The batch or the stream did not run yet. Start `energy_batch_pipeline` in Airflow, or run `make backfill` |
| The health DAG fails | `serving.pipeline_health` | Hours that are due are missing (all of today from 05:00 Berlin time). Possible causes: the 03:00 backfill failed, the hourly transform stopped, or SMARD published late. The detail shows the newest hour and the hours that were due |
| A port is already in use | `netstat -ano \| findstr 8088` (Windows) | Another Postgres or Airflow instance uses the port. Stop it, or change the ports in `docker-compose.yml` |
| Docker Desktop does not have sufficient memory | Settings → Resources | Give a minimum of 8 GB RAM to Docker. The Spark worker uses a maximum of 2 GB |
| The news job keeps NULL categories | The `make news` output | classifier.dev is not available or limits the requests. The job keeps the headlines. The next run gives them a topic (ADR 0003) |

## Table maintenance

Each streaming micro-batch and each hourly MERGE makes a new Iceberg snapshot.
Each one also writes small data files. The `energy_table_maintenance` DAG runs
`spark/jobs/maintain_tables.py` each day at 04:30 Europe/Berlin. This time is
after the 03:00 backfill and between two hourly transforms.

For each table in `lake.energy`, the job does these steps:

1. It runs `rewrite_data_files`. This merges the small files in each
   partition (bin-packing). Partial progress is on. If a file group has a
   conflict with a streaming write, the job skips that group. The next run
   merges it.
2. It runs `expire_snapshots`. This removes the snapshots that are older than
   7 days, but it always keeps the newest 10. It also deletes the data files
   that no snapshot uses.

Thus, time travel goes back a minimum of 7 days. Compaction never changes rows.
`tests/test_table_maintenance.py` checks this. To see the result, use these
queries:

```sql
SELECT count(*) FROM lake.energy.bronze_prices.files;      -- data files
SELECT count(*) FROM lake.energy.bronze_prices.snapshots;  -- snapshots kept
```

For each table, the job shows how many files it merged and how many it
deleted.

After a successful rewrite, Iceberg 1.8.1 can show this message:
"partial-progress.enabled is true but N rewrite commits failed". This message
is not a failure count. In the tests, N was 9 after one successful commit
(`partial-progress.max-commits` is 10). Use the counts that the job shows.

All other errors make the task fail. Airflow then tries the task two more
times.

## Alerting

You can add alerts without a change to the code:

- **Airflow** makes `energy_healthcheck` fail when hours that are due are
  missing from the serving layer. From 05:00 Berlin time, all hours of today
  are due. To send a notification, add a notifier through the DAG
  `default_args` (`on_failure_callback`) or through an Airflow provider. The
  repository has no notifier and no credentials.
- **GitHub Actions** opens an issue when the scheduled `market-summary` run
  fails. It does not open a second issue while one is open.
- **Grafana** shows `serving.pipeline_health`. Thus, you can see an old
  pipeline state on the dashboard before an alert starts.

## Data semantics worth remembering

- **`null` prices in SMARD responses are normal.** They are future hours of the
  current week. They are not lost data. The parser ignores them.
- **Revisions are normal.** Each ingestion is an upsert with the key
  `(region, delivery_ts)`. The row with the newest `fetched_at` stays. Thus, a
  replay of data is always safe (refer to
  `docs/decisions/0002-revision-aware-upserts.md`).
- **Timestamps** are in UTC. The silver layer calculates the Berlin local
  hour. The weekend, negative-price and peak flags are also in silver.
