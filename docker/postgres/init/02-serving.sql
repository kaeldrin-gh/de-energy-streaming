\connect serving

-- Serving layer: small, query-friendly tables refreshed by the batch jobs.
-- Grafana reads from here; simple SQL works too.

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.price_hourly (
    region         text        NOT NULL,
    delivery_ts    timestamptz NOT NULL,
    price_eur_mwh  numeric(10, 2),
    local_hour     int,
    is_weekend     boolean,
    is_negative    boolean,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (region, delivery_ts)
);

CREATE TABLE IF NOT EXISTS serving.daily_stats (
    region         text        NOT NULL,
    local_day      date        NOT NULL,
    avg_price      numeric(10, 2),
    min_price      numeric(10, 2),
    max_price      numeric(10, 2),
    negative_hours int,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (region, local_day)
);

-- Written by the Airflow health-check DAG, surfaced on the Grafana dashboard.
CREATE TABLE IF NOT EXISTS serving.pipeline_health (
    check_name text PRIMARY KEY,
    status     text NOT NULL,  -- ok | warn | fail
    detail     text,
    checked_at timestamptz NOT NULL DEFAULT now()
);
