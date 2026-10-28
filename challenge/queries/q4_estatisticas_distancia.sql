-- 4. Média, desvio padrão, mínimo, máximo e quartis da distância percorrida (milhas).
SELECT ROUND(AVG(trip_distance), 4)          AS media,
       ROUND(STDDEV_SAMP(trip_distance), 4)  AS desvio_padrao,
       MIN(trip_distance)                    AS minimo,
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1_25,
       PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_distance) AS q2_mediana,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3_75,
       MAX(trip_distance)                    AS maximo
FROM refined.trips;
