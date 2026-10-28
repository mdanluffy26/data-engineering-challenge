-- 3. Qual foi o dia da viagem mais longa percorrida?
-- "Mais longa percorrida" = maior distância (trip_distance, em milhas).
SELECT pickup_date AS dia,
       pickup_datetime,
       dropoff_datetime,
       trip_distance,
       trip_duration_min
FROM refined.trips
ORDER BY trip_distance DESC, pickup_datetime
LIMIT 1;
