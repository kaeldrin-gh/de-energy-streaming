"""Kafka -> Iceberg bronze streaming job.

Run against the local cluster:

    spark-submit --master spark://spark-master:7077 /opt/jobs/stream_prices.py

Semantics:
* Kafka gives at-least-once; the MERGE into Iceberg makes writes idempotent,
  so restarts and replayed history converge to the same state (ADR 0002).
* Malformed or incomplete records are routed to the DLQ topic instead of
  being silently dropped.
* Offsets are checkpointed in S3 (LocalStack locally), separate per query.
"""

from __future__ import annotations

import os

from common import build_spark, ensure_tables, env, merge_bronze_from_view
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructType

MESSAGE_SCHEMA = (
    StructType()
    .add("region", StringType())
    .add("delivery_ts", StringType())
    .add("price_eur_mwh", DoubleType())
    .add("source", StringType())
    .add("fetched_at", StringType())
)


def main() -> None:
    spark = build_spark("stream_prices")
    ensure_tables(spark)

    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    topic = env("KAFKA_TOPIC_PRICES", "energy.prices.raw")
    dlq_topic = env("KAFKA_TOPIC_DLQ", "energy.prices.invalid")
    checkpoint_base = env("CHECKPOINT_BASE", "s3a://energy-lake/checkpoints")
    trigger = os.environ.get("TRIGGER_INTERVAL", "30 seconds")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("d"),
        F.col("timestamp").alias("kafka_ts"),
    ).select("d.*", "kafka_ts")

    is_valid = (
        F.col("region").isNotNull()
        & F.col("delivery_ts").isNotNull()
        & F.col("price_eur_mwh").isNotNull()
        & F.col("fetched_at").isNotNull()
    )

    valid = (
        parsed.filter(is_valid)
        .withColumn("delivery_ts", F.to_timestamp("delivery_ts"))
        .withColumn("fetched_at", F.to_timestamp("fetched_at"))
        .filter(F.col("delivery_ts").isNotNull() & F.col("fetched_at").isNotNull())
        .drop("kafka_ts")
    )
    invalid = parsed.filter(~is_valid)

    def upsert_bronze(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        view = f"bronze_batch_{batch_id}"
        batch_df.createOrReplaceTempView(view)
        merge_bronze_from_view(spark, view)
        spark.catalog.dropTempView(view)

    def route_to_dlq(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        payload = F.to_json(F.struct(*[F.col(column) for column in batch_df.columns]))
        (
            batch_df.select(payload.alias("value"))
            .write.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("topic", dlq_topic)
            .save()
        )

    bronze_query = (
        valid.writeStream.foreachBatch(upsert_bronze)
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/stream_prices_bronze")
        .trigger(processingTime=trigger)
        .start()
    )

    dlq_query = (
        invalid.writeStream.foreachBatch(route_to_dlq)
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/stream_prices_dlq")
        .trigger(processingTime=trigger)
        .start()
    )

    print(
        f"stream_prices running: bronze query id={bronze_query.id}, "
        f"dlq query id={dlq_query.id} - waiting for new Kafka records"
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
