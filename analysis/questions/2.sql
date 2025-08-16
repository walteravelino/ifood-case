USE
nyc_taxi.processed;

-- 2) Média de passageiros (passenger_count) por cada hora do dia no mês de maio considerando todos os táxis da frota:
SELECT
pickup_hour,
ROUND(avg_passengers, 2) AS avg_passengers
FROM gold_hourly_avg_passengers_may
ORDER BY pickup_hour ASC;