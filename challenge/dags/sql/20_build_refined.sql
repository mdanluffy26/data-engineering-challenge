-- REFINED: tabela final de viagens, pronta para análise, e um agregado diário.
-- Reconstruída por completo a cada execução (o custo é baixo perto da carga raw).

DROP TABLE IF EXISTS refined.trips;
CREATE TABLE refined.trips AS
SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_datetime::DATE  AS pickup_date,
    dropoff_datetime::DATE AS dropoff_date,
    ROUND(EXTRACT(EPOCH FROM dropoff_datetime - pickup_datetime) / 60, 2)::NUMERIC(10, 2)
        AS trip_duration_min,
    passenger_count,
    trip_distance,
    ROUND(trip_distance / (EXTRACT(EPOCH FROM dropoff_datetime - pickup_datetime) / 3600), 2)::NUMERIC(10, 2)
        AS avg_speed_mph,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    source_file
FROM trusted.yellow_tripdata;

CREATE INDEX ix_refined_trips_pickup_date   ON refined.trips (pickup_date);
CREATE INDEX ix_refined_trips_trip_distance ON refined.trips (trip_distance);

DROP TABLE IF EXISTS refined.daily_trips;
CREATE TABLE refined.daily_trips AS
SELECT
    pickup_date,
    count(*)                        AS trips,
    SUM(passenger_count)            AS passengers,
    ROUND(AVG(trip_distance), 2)    AS avg_distance,
    ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
    ROUND(SUM(total_amount), 2)     AS total_amount
FROM refined.trips
GROUP BY pickup_date;

ANALYZE refined.trips;
ANALYZE refined.daily_trips;
