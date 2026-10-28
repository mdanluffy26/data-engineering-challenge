-- 2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?
-- Considera a viagem que começou E terminou em 17/06/2022.
SELECT count(*) AS viagens_17_junho
FROM refined.trips
WHERE pickup_date  = DATE '2022-06-17'
  AND dropoff_date = DATE '2022-06-17';
