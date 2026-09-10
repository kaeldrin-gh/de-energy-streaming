# Airflow image for batch orchestration (LocalExecutor).
#
# Why a custom image:
# * SparkSubmitOperator runs the PySpark driver *inside this container* in
#   client mode, so the driver needs a JRE, pyspark, and the same Python
#   minor version as the executors in apache/spark:3.5.9-python3 (Ubuntu 22.04
#   ships Python 3.10 - hence the 2.10.5-python3.10 base tag).
# * The driver also resolves Iceberg/Kafka/S3 jars via `--packages` on first
#   run (cached in a named volume), because client mode needs those on the
#   driver classpath as well as the workers'.
FROM apache/airflow:2.10.5-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY docker/airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && ln -sf "$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "bin", "spark-submit"))')" \
        /home/airflow/.local/bin/spark-submit

# Pre-create mount points so named volumes inherit airflow ownership.
RUN mkdir -p /opt/airflow/logs /home/airflow/.ivy2
