USE
nyc_taxi.processed;

-- 2) Média de passageiros (passenger_count) por cada hora do dia no mês de maio considerando todos os táxis da frota:
SELECT *
FROM gold_hourly_avg_passengers_may
ORDER BY pickup_hour ASC;