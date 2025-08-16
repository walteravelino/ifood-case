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

    try:
        # Leitura dos dados
        df = pipeline.read_landing_data(paths.landing)

        # Processamento para silver
        silver_df = pipeline.process_to_silver(df)

        # Persistência
        (silver_df.write
         .format("delta")
         .mode("overwrite")
         .partitionBy("pickup_year", "pickup_month")
         .saveAsTable(tables.silver))

        # Criação das tabelas gold
        pipeline.create_gold_tables(silver_df, tables)

        print("Pipeline executado com sucesso!")

    except Exception as e:
        print(f"Erro durante a execução do pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
