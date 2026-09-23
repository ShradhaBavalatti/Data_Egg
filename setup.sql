-- =====================================================================
-- setup.sql — provisions the source (operational) and warehouse schemas
-- for the NYC Taxi real-time streaming pipeline.
--
-- Run once:  psql -d taxi_stream -f setup.sql
-- (the taxi_stream database itself is created by 00_load_source.py / run_pipeline.py)
-- =====================================================================

-- --------------------------------------------------------------------
-- Schemas:  taxi_src = where the stream originates (the "source system")
--           taxi_dw  = analytics warehouse where curated Gold data lands
-- --------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS taxi_src;
CREATE SCHEMA IF NOT EXISTS taxi_dw;

-- --------------------------------------------------------------------
-- Dimension: taxi zones (the small lookup table → broadcast join in Spark)
-- --------------------------------------------------------------------
DROP TABLE IF EXISTS taxi_src.zones CASCADE;
CREATE TABLE taxi_src.zones (
    location_id   INTEGER PRIMARY KEY,
    borough       TEXT,
    zone          TEXT,
    service_zone  TEXT
);

-- --------------------------------------------------------------------
-- Source fact table: taxi trips.
--   * trip_id      — monotonically increasing surrogate PK  → incremental cursor
--   * trip_key     — deterministic business key (md5)        → dedup key downstream
--   * last_updated — row mutation timestamp                  → timestamp-cursor option
-- The natural TLC feed has no single primary key, so the surrogate PK we add here is
-- exactly what enables deterministic incremental extraction (offset-based is avoided).
-- --------------------------------------------------------------------
DROP TABLE IF EXISTS taxi_src.trips CASCADE;
CREATE TABLE taxi_src.trips (
    trip_id                BIGSERIAL PRIMARY KEY,
    trip_key               TEXT        NOT NULL,
    vendor_id              INTEGER,
    tpep_pickup_datetime   TIMESTAMP   NOT NULL,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        INTEGER,
    trip_distance          DOUBLE PRECISION,
    ratecode_id            INTEGER,
    store_and_fwd_flag     TEXT,
    pu_location_id         INTEGER,
    do_location_id         INTEGER,
    payment_type           INTEGER,
    fare_amount            DOUBLE PRECISION,
    extra                  DOUBLE PRECISION,
    mta_tax                DOUBLE PRECISION,
    tip_amount             DOUBLE PRECISION,
    tolls_amount           DOUBLE PRECISION,
    improvement_surcharge  DOUBLE PRECISION,
    total_amount           DOUBLE PRECISION,
    congestion_surcharge   DOUBLE PRECISION,
    airport_fee            DOUBLE PRECISION,
    last_updated           TIMESTAMP   NOT NULL DEFAULT now()
);

-- Indexes that make incremental extraction cheap (cursor columns).
CREATE INDEX IF NOT EXISTS idx_trips_trip_id       ON taxi_src.trips (trip_id);
CREATE INDEX IF NOT EXISTS idx_trips_last_updated  ON taxi_src.trips (last_updated);

-- --------------------------------------------------------------------
-- Warehouse (Gold) targets — business-ready outputs loaded back from Spark.
-- Idempotent upserts use these natural keys (ON CONFLICT ... DO UPDATE).
-- --------------------------------------------------------------------

-- Fact: validated, deduplicated trips loaded back from the Silver layer.
-- Natural key = trip_key (unique after watermarked dedup) → ON CONFLICT DO NOTHING.
DROP TABLE IF EXISTS taxi_dw.fact_trips CASCADE;
CREATE TABLE taxi_dw.fact_trips (
    trip_id                BIGINT,
    trip_key               TEXT PRIMARY KEY,
    vendor_id              INTEGER,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        INTEGER,
    trip_distance          DOUBLE PRECISION,
    pu_location_id         INTEGER,
    do_location_id         INTEGER,
    payment_type           INTEGER,
    fare_amount            DOUBLE PRECISION,
    tip_amount             DOUBLE PRECISION,
    total_amount           DOUBLE PRECISION,
    tip_pct                DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_fact_pickup ON taxi_dw.fact_trips (tpep_pickup_datetime);

-- Gold 1: hourly demand & revenue by pickup borough
DROP TABLE IF EXISTS taxi_dw.gold_borough_hourly CASCADE;
CREATE TABLE taxi_dw.gold_borough_hourly (
    pickup_borough   TEXT,
    window_start     TIMESTAMP,
    trips            BIGINT,
    total_revenue    DOUBLE PRECISION,
    avg_fare         DOUBLE PRECISION,
    avg_distance     DOUBLE PRECISION,
    avg_tip_pct      DOUBLE PRECISION,
    PRIMARY KEY (pickup_borough, window_start)
);

-- Gold 2: top origin→destination zone flows
DROP TABLE IF EXISTS taxi_dw.gold_zone_flows CASCADE;
CREATE TABLE taxi_dw.gold_zone_flows (
    pickup_zone      TEXT,
    dropoff_zone     TEXT,
    trips            BIGINT,
    avg_fare         DOUBLE PRECISION,
    avg_distance     DOUBLE PRECISION,
    PRIMARY KEY (pickup_zone, dropoff_zone)
);

-- Gold 3: payment-type mix by day
DROP TABLE IF EXISTS taxi_dw.gold_payment_daily CASCADE;
CREATE TABLE taxi_dw.gold_payment_daily (
    trip_date        DATE,
    payment_type     INTEGER,
    payment_label    TEXT,
    trips            BIGINT,
    total_revenue    DOUBLE PRECISION,
    PRIMARY KEY (trip_date, payment_type)
);

-- Run-level metrics captured by validate.py / benchmark.py for the performance report.
DROP TABLE IF EXISTS taxi_dw.pipeline_runs CASCADE;
CREATE TABLE taxi_dw.pipeline_runs (
    run_id           TEXT,
    profile          TEXT,
    stage            TEXT,
    records          BIGINT,
    duration_seconds DOUBLE PRECISION,
    throughput_rps   DOUBLE PRECISION,
    captured_at      TIMESTAMP DEFAULT now(),
    PRIMARY KEY (run_id, stage)
);
