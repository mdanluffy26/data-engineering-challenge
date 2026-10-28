-- Camadas de processamento
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS refined;

-- RAW: dados exatamente como vêm do arquivo, mais metadados de carga
CREATE TABLE IF NOT EXISTS raw.yellow_tripdata (
    vendorid              BIGINT,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       DOUBLE PRECISION,
    trip_distance         DOUBLE PRECISION,
    ratecodeid            DOUBLE PRECISION,
    store_and_fwd_flag    TEXT,
    pulocationid          BIGINT,
    dolocationid          BIGINT,
    payment_type          BIGINT,
    fare_amount           DOUBLE PRECISION,
    extra                 DOUBLE PRECISION,
    mta_tax               DOUBLE PRECISION,
    tip_amount            DOUBLE PRECISION,
    tolls_amount          DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount          DOUBLE PRECISION,
    congestion_surcharge  DOUBLE PRECISION,
    airport_fee           DOUBLE PRECISION,
    source_file           TEXT NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_yellow_tripdata_source_file
    ON raw.yellow_tripdata (source_file);

-- TRUSTED: dados tipados, renomeados e filtrados pelas regras de qualidade
CREATE TABLE IF NOT EXISTS trusted.yellow_tripdata (
    vendor_id             SMALLINT,
    pickup_datetime       TIMESTAMP NOT NULL,
    dropoff_datetime      TIMESTAMP NOT NULL,
    passenger_count       SMALLINT,
    trip_distance         NUMERIC(10, 2) NOT NULL,
    ratecode_id           SMALLINT,
    store_and_fwd_flag    BOOLEAN,
    pu_location_id        SMALLINT,
    do_location_id        SMALLINT,
    payment_type          SMALLINT,
    fare_amount           NUMERIC(10, 2),
    extra                 NUMERIC(10, 2),
    mta_tax               NUMERIC(10, 2),
    tip_amount            NUMERIC(10, 2),
    tolls_amount          NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount          NUMERIC(10, 2),
    congestion_surcharge  NUMERIC(10, 2),
    airport_fee           NUMERIC(10, 2),
    source_file           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_trusted_yellow_tripdata_source_file
    ON trusted.yellow_tripdata (source_file);

-- Auditoria: quantas linhas cada arquivo gerou em cada camada
CREATE TABLE IF NOT EXISTS trusted.load_audit (
    source_file  TEXT PRIMARY KEY,
    raw_rows     BIGINT NOT NULL,
    trusted_rows BIGINT NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
