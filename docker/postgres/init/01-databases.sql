-- Runs once on first Postgres start (docker-entrypoint-initdb.d).
-- POSTGRES_USER / POSTGRES_DB default to `energy`.

CREATE DATABASE airflow OWNER energy;  -- Airflow metadata
CREATE DATABASE iceberg OWNER energy;  -- Iceberg JDBC catalog
CREATE DATABASE serving OWNER energy;  -- BI / serving layer
