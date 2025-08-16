from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, to_date, year, month, hour, avg
from typing import List


class TaxiPipeline:
    """Classe para processar dados de táxi no Databricks com schema flexível"""

    UNIFIED_SCHEMA = {
        'VendorID': LongType(),
        'passenger_count': DoubleType(),
        'trip_distance': DoubleType(),
        'RatecodeID': DoubleType(),
        'store_and_fwd_flag': StringType(),
        'PULocationID': LongType(),
        'DOLocationID': LongType(),
        'payment_type': DoubleType(),
        'fare_amount': DoubleType(),
        'extra': DoubleType(),
        'mta_tax': DoubleType(),
        'tip_amount': DoubleType(),
        'tolls_amount': DoubleType(),
        'improvement_surcharge': DoubleType(),
        'total_amount': DoubleType(),
        'congestion_surcharge': DoubleType(),
        'pickup_datetime': TimestampType(),
        'dropoff_datetime': TimestampType(),
        'airport_fee': DoubleType(),
        'ehail_fee': DoubleType(),
        'trip_type': DoubleType(),
        'taxi_type': StringType()
    }

    def __init__(self, spark):
        self.spark = spark
        # Inicialização segura do dbutils para Databricks
        try:
            self.dbutils = spark._jvm.com.databricks.service.DBUtils
        except:
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(spark)

    def _standardize_dataframe(self, df: DataFrame) -> DataFrame:
        """Padroniza o DataFrame para o schema unificado"""
        try:
            is_yellow = 'tpep_pickup_datetime' in df.columns
            taxi_type = 'yellow' if is_yellow else 'green'

            pickup_col = 'tpep_pickup_datetime' if is_yellow else 'lpep_pickup_datetime'
            dropoff_col = 'tpep_dropoff_datetime' if is_yellow else 'lpep_dropoff_datetime'

            df = (df
                  .withColumn('pickup_datetime', col(pickup_col))
                  .withColumn('dropoff_datetime', col(dropoff_col))
                  .withColumn('taxi_type', lit(taxi_type))
                  .drop(pickup_col, dropoff_col))

            for col_name, col_type in self.UNIFIED_SCHEMA.items():
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None).cast(col_type))
                else:
                    df = df.withColumn(col_name, col(col_name).cast(col_type))

            return df.select(*self.UNIFIED_SCHEMA.keys())

        except Exception as e:
            raise ValueError(f"Erro na padronização do DataFrame: {str(e)}")

    def read_landing_data(self, landing_path: str) -> DataFrame:
        """Lê dados da zona de landing"""
        try:
            # Lista arquivos Parquet no diretório de landing
            file_infos = self.dbutils.fs.ls(landing_path)
            parquet_files = [fi.path for fi in file_infos if fi.path.endswith('.parquet')]

            if not parquet_files:
                raise ValueError(f"Nenhum arquivo Parquet encontrado em {landing_path}")

            # Lê o primeiro arquivo para iniciar o DataFrame combinado
            combined_df = self._standardize_dataframe(self.spark.read.parquet(parquet_files[0]))

            # Adiciona os demais arquivos
            for path in parquet_files[1:]:
                df = self._standardize_dataframe(self.spark.read.parquet(path))
                combined_df = combined_df.unionByName(df)

            return combined_df

        except Exception as e:
            raise ValueError(f"Erro ao ler dados da landing zone: {str(e)}")

    def process_to_silver(self, df: DataFrame) -> DataFrame:
        """Processamento para silver layer com validações"""
        return (df
                .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0))
                .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
                .withColumn("pickup_date", to_date("pickup_datetime"))
                .withColumn("pickup_year", year("pickup_datetime"))
                .withColumn("pickup_month", month("pickup_datetime"))
                .withColumn("pickup_hour", hour("pickup_datetime")))

    def create_gold_tables(self, silver_df: DataFrame, tables) -> None:
        """Cria as tabelas gold no Unity Catalog"""
        # Tabela gold_monthly_avg_amount
        (silver_df.filter(col("taxi_type") == "yellow")
         .groupBy("pickup_year", "pickup_month")
         .agg(avg("total_amount").alias("avg_amount"))
         .write.format("delta")
         .mode("overwrite")
         .saveAsTable(tables.gold_monthly))

        # Tabela gold_hourly_avg_passengers_may (apenas maio)
        (silver_df.filter(col("pickup_month") == 5)
         .groupBy("pickup_hour")
         .agg(avg("passenger_count").alias("avg_passengers"))
         .write.format("delta")
         .mode("overwrite")
         .saveAsTable(tables.gold_hourly))
