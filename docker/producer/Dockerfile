# Standalone producer image: SMARD.de -> Kafka.
# Also used for offline replay of the bundled sample data (no network needed).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY producer ./producer

RUN pip install --no-cache-dir .

COPY data ./data
COPY scripts ./scripts

ENTRYPOINT ["python", "-m", "producer"]
CMD ["--help"]
