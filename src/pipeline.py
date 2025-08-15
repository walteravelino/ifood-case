from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit, to_date, year, month, hour, avg
from src.config import Paths, Tables


class TaxiPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_without_schema(self, path: str) -> DataFrame:
        """Lê arquivos Parquet sem schema pré-definido e converte colunas manualmente"""
        df = self.spark.read.parquet(path)

        column_types = {
            'VendorID': IntegerType(),
            'lpep_pickup_datetime': TimestampType(),
            'lpep_dropoff_datetime': TimestampType(),
            'passenger_count': IntegerType(),
            'trip_distance': DoubleType(),
            'total_amount': DoubleType(),
            'PULocationID': IntegerType(),
            'DOLocationID': IntegerType(),
            'payment_type': IntegerType()
        }

        selected_cols = []
        for col_name, col_type in column_types.items():
            if col_name in df.columns:
                selected_cols.append(col(col_name).cast(col_type).alias(col_name))

        return df.select(*selected_cols)

    def process_to_silver(self, df: DataFrame, taxi_type: str) -> DataFrame:
        """Processamento para silver layer com tratamento"""
        return (df
                .withColumn("taxi_type", lit(taxi_type))
                .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0))
                .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
                .withColumn("pickup_date", to_date("lpep_pickup_datetime"))
                .withColumn("pickup_year", year("lpep_pickup_datetime"))
                .withColumn("pickup_month", month("lpep_pickup_datetime"))
                .withColumn("pickup_hour", hour("lpep_pickup_datetime")))

    def create_gold_tables(self, silver_df: DataFrame, tables: Tables):
        """Cria as tabelas gold com verificações adicionais"""
        (silver_df.filter(col("taxi_type") == "yellow")
         .groupBy("pickup_year", "pickup_month")
         .agg(avg("total_amount").alias("avg_amount"))
         .write.format("delta")
         .mode("overwrite")
         .saveAsTable(tables.gold_monthly))

        (silver_df.filter(col("pickup_month") == 5)
         .groupBy("pickup_hour")
         .agg(avg("passenger_count").alias("avg_passengers"))
         .write.format("delta")
         .mode("overwrite")
         .saveAsTable(tables.gold_hourly))
