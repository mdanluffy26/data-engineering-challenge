-- Carrega um arquivo (mês) da camada raw para a trusted.
-- Parâmetros: %(source_file)s, %(month_start)s, %(month_end)s
--
-- Regras de qualidade (ver README):
--   1. embarque dentro do mês do arquivo (remove datas de 2001, 2008, 2023...)
--   2. duração > 0 e <= 24 horas
--   3. distância > 0
--   4. velocidade média <= 100 mph (remove distâncias absurdas, ex.: 389 mil milhas)
--   5. linhas idênticas são contadas uma única vez (DISTINCT)

DELETE FROM trusted.yellow_tripdata WHERE source_file = %(source_file)s;

INSERT INTO trusted.yellow_tripdata
SELECT DISTINCT
    vendorid::SMALLINT,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count::SMALLINT,
    trip_distance::NUMERIC(10, 2),
    ratecodeid::SMALLINT,
    CASE store_and_fwd_flag WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END,
    pulocationid::SMALLINT,
    dolocationid::SMALLINT,
    payment_type::SMALLINT,
    fare_amount::NUMERIC(10, 2),
    extra::NUMERIC(10, 2),
    mta_tax::NUMERIC(10, 2),
    tip_amount::NUMERIC(10, 2),
    tolls_amount::NUMERIC(10, 2),
    improvement_surcharge::NUMERIC(10, 2),
    total_amount::NUMERIC(10, 2),
    congestion_surcharge::NUMERIC(10, 2),
    airport_fee::NUMERIC(10, 2),
    source_file
FROM raw.yellow_tripdata
WHERE source_file = %(source_file)s
  AND tpep_pickup_datetime >= %(month_start)s
  AND tpep_pickup_datetime <  %(month_end)s
  AND tpep_dropoff_datetime >  tpep_pickup_datetime
  AND tpep_dropoff_datetime <= tpep_pickup_datetime + INTERVAL '24 hours'
  AND trip_distance > 0
  AND trip_distance / (EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime) / 3600) <= 100;

INSERT INTO trusted.load_audit (source_file, raw_rows, trusted_rows)
SELECT %(source_file)s,
       (SELECT count(*) FROM raw.yellow_tripdata     WHERE source_file = %(source_file)s),
       (SELECT count(*) FROM trusted.yellow_tripdata WHERE source_file = %(source_file)s)
ON CONFLICT (source_file) DO UPDATE
   SET raw_rows = EXCLUDED.raw_rows,
       trusted_rows = EXCLUDED.trusted_rows,
       loaded_at = now();
