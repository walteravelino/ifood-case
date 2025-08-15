from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    base: str = "dbfs:/Volumes/workspace/default/nyc"
    landing: str = f"{base}/landing"
    bronze: str = f"{base}/bronze"
    silver: str = f"{base}/silver"
    gold: str = f"{base}/gold"

@dataclass(frozen=True)
class Tables:
    catalog: str = "nyc_taxi"  # Catálogo UC (criará automaticamente)
    db: str = "processed"
    bronze: str = f"{catalog}.{db}.trips_bronze"
    silver: str = f"{catalog}.{db}.trips_silver"
    gold_monthly: str = f"{catalog}.{db}.gold_monthly_avg_amount"
    gold_hourly: str = f"{catalog}.{db}.gold_hourly_avg_passengers_may"
