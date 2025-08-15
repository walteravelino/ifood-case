USE
nyc_taxi.processed;

-- 1) Média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota:
SELECT *
FROM gold_monthly_avg_amount

-- 2) Média de passageiros (passenger_count) por cada hora do dia no mês de maio considerando todos os táxis da frota:
SELECT *
FROM gold_hourly_avg_passengers_may