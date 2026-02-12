"""
Ingesta batch de pharma-data.csv a Bronze (Delta Lake).

Transforma nombres de columnas a snake_case y preserva datos originales.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


# CONFIGURACIÓN
LANDING_PATH: str = "/data/landing/pharma-data.csv"
BRONZE_PATH: str = "/data/bronze/pharma_sales_raw"


def create_spark_session() -> SparkSession:
    """Crea sesión Spark con soporte Delta Lake."""
    return (
        SparkSession.builder
        .appName("ingest_pharma_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


def normalize_column_name(name: str) -> str:
    """Convierte nombre de columna a snake_case."""
    return (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )


def ingest_to_bronze(spark: SparkSession) -> int:
    """
    Lee CSV de landing y escribe a Bronze Delta.
    
    Returns:
        Número de registros ingestados.
    """
    print(f"[bronze] Leyendo CSV desde: {LANDING_PATH}")
    
    # Leer CSV con inferencia de esquema
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(LANDING_PATH)
    )
    
    # Normalizar nombres de columnas a snake_case
    for old_name in df.columns:
        new_name = normalize_column_name(old_name)
        df = df.withColumnRenamed(old_name, new_name)
    
    # Añadir metadatos de ingesta
    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("pharma-data.csv"))
    )
    
    record_count = df.count()
    print(f"[bronze] Registros leídos: {record_count:,}")
    
    # Mostrar esquema
    print("\n[bronze] Esquema:")
    df.printSchema()
    
    # Escribir a Delta
    print(f"\n[bronze] Escribiendo a: {BRONZE_PATH}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
    )
    
    print(f"[bronze] ✓ {record_count:,} registros escritos a Delta")
    return record_count


def validate_bronze(spark: SparkSession) -> None:
    """Validaciones básicas post-ingesta."""
    print("\n[bronze] Validando tabla...")
    
    df = spark.read.format("delta").load(BRONZE_PATH)
    
    total = df.count()
    nulls_quantity = df.filter(col("quantity").isNull()).count()
    nulls_price = df.filter(col("price").isNull()).count()
    duplicates = total - df.dropDuplicates().count()
    
    print(f"  - Total registros: {total:,}")
    print(f"  - Nulos en quantity: {nulls_quantity:,}")
    print(f"  - Nulos en price: {nulls_price:,}")
    print(f"  - Duplicados exactos: {duplicates:,}")
    
    # Muestra de datos
    print("\n[bronze] Muestra (3 registros):")
    df.select(
        "distributor", "customer_name", "product_name", 
        "quantity", "price", "sales", "channel"
    ).show(3, truncate=30)


def main() -> None:
    """Punto de entrada principal."""
    print("=" * 60)
    print("[bronze] INICIO - Ingesta pharma-data.csv")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        ingest_to_bronze(spark)
        validate_bronze(spark)
        
        print("=" * 60)
        print("[bronze] FIN - Ingesta completada exitosamente")
        print("=" * 60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
