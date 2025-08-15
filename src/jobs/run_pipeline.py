from pyspark.sql import SparkSession
from src.config import Paths, Tables
from src.pipeline import TaxiPipeline


def get_spark():
    return (SparkSession.builder
            .appName("NYCTaxiPipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def main():
    spark = get_spark()
    pipeline = TaxiPipeline(spark)
    paths, tables = Paths(), Tables()

    # Leitura sem schema
    yellow_df = pipeline.read_without_schema(f"{paths.landing}/yellow_tripdata_*.parquet")
    green_df = pipeline.read_without_schema(f"{paths.landing}/green_tripdata_*.parquet")

    # Processamento
    silver_df = (pipeline.process_to_silver(yellow_df, "yellow")
                 .unionByName(pipeline.process_to_silver(green_df, "green")))

    # Persistência
    (silver_df.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("pickup_year", "pickup_month")
     .saveAsTable(tables.silver))

    # Criação das tabelas gold
    pipeline.create_gold_tables(silver_df, tables)


if __name__ == "__main__":
    main()
