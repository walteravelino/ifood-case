USE
nyc_taxi.processed;

-- 1) Média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota:
SELECT
    pickup_year,
    pickup_month,
    avg_amount,
    CONCAT(CAST(pickup_year AS STRING), '-', LPAD(CAST(pickup_month AS STRING), 2, '0')) AS year_month
FROM gold_monthly_avg_amount
ORDER BY
    year_month DESC;