# Spark runtime for the streaming and batch jobs.
# Base image version is pinned; connectors are baked in so jobs start without
# downloading packages at runtime (works offline, faster CI/dev loops).
FROM apache/spark:3.5.9-python3

USER root

ARG ICEBERG_VERSION=1.8.1
ARG KAFKA_CONNECTOR_VERSION=3.5.9
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.3

# Iceberg (lakehouse), Kafka connector (Structured Streaming), S3A client
# (LocalStack/MinIO/AWS S3), Postgres JDBC (serving writes).
RUN set -eux; \
    curl -fsSL -o "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/spark-sql-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# Python deps for the driver/executor UDFs and serving writes.
# numpy is pinned below 2.0 (Spark 3.5 compatibility).
RUN python3 -m pip install --no-cache-dir \
        "numpy==1.26.4" \
        "pandas==2.2.3" \
        "pyarrow==16.1.0" \
        "requests==2.32.3" \
        "psycopg2-binary==2.9.9"

# Jobs and the shared producer package (SMARD client is reused by the backfill job).
COPY spark/jobs /opt/jobs
COPY producer /opt/producer
COPY spark/conf/metrics.properties /opt/spark/conf/metrics.properties

ENV PYTHONPATH=/opt:/opt/jobs

WORKDIR /opt/spark

# The Kafka connector jar is NOT fat: it needs kafka-clients and commons-pool2
# on the classpath, and Spark does not ship either. Versions are taken from the
# connector POM (spark-sql-kafka-0-10_2.12:3.5.9 -> kafka-clients:3.4.1,
# commons-pool2:2.11.1). Kept as the last layer so rebuilds stay cheap.
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG SPARK_VERSION=3.5.9

RUN set -eux; \
    curl -fsSL -o "/opt/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    curl -fsSL -o "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"
