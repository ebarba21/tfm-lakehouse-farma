"""
Ingesta batch de Suppy_Chain_Shipment_Data.csv a Bronze (Delta Lake).

Este dataset tiene fechas en formatos inconsistentes (texto como "Pre-PQ Process", 
"Date Not Captured", etc.) que se preservan en Bronze y se limpiarán en Silver.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


# CONFIGURACIÓN
LANDING_PATH: str = "/data/landing/Suppy_Chain_Shipment_Data.csv"
BRONZE_PATH: str = "/data/bronze/shipments_raw"


def create_spark_session() -> SparkSession:
    """Crea sesión Spark con soporte Delta Lake."""
    return (
        SparkSession.builder
        .appName("ingest_shipments_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


def normalize_column_name(name: str) -> str:
    """
    Convierte nombre de columna a snake_case.
    
    Maneja casos especiales como "pq #", "po / so #", "(per pack)".
    """
    return (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("#", "num")
        .replace("(", "")
        .replace(")", "")
        .replace("__", "_")  # limpiar dobles guiones bajos
        .strip("_")
    )


def ingest_to_bronze(spark: SparkSession) -> int:
    """
    Lee CSV de landing y escribe a Bronze Delta.
    
    NOTA: Todas las columnas se leen como STRING para preservar
    valores problemáticos (fechas con texto, etc.) que se limpiarán en Silver.
    
    Returns:
        Número de registros ingestados.
    """
    print(f"[bronze] Leyendo CSV desde: {LANDING_PATH}")
    
    # Leer CSV - TODO como string para preservar datos problemáticos
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # Todo como string
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
        .withColumn("_source_file", lit("Suppy_Chain_Shipment_Data.csv"))
    )
    
    record_count = df.count()
    print(f"[bronze] Registros leídos: {record_count:,}")
    
    # Mostrar esquema
    print("\n[bronze] Esquema:")
    df.printSchema()
    
    # Mostrar columnas (este dataset tiene muchas)
    print(f"\n[bronze] Total columnas: {len(df.columns)}")
    
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
    
    # Contar valores problemáticos en columnas de fecha
    date_cols = [
        "pq_first_sent_to_client_date",
        "po_sent_to_vendor_date", 
        "scheduled_delivery_date",
        "delivered_to_client_date",
        "delivery_recorded_date"
    ]
    
    print(f"  - Total registros: {total:,}")
    print("\n  Valores no-fecha en columnas de fecha:")
    
    for date_col in date_cols:
        if date_col in df.columns:
            non_dates = df.filter(
                (col(date_col).contains("Process")) |
                (col(date_col).contains("Captured")) |
                (col(date_col).isNull())
            ).count()
            print(f"    - {date_col}: {non_dates:,} valores problemáticos")
    
    # Muestra de datos
    print("\n[bronze] Muestra (3 registros):")
    df.select(
        "id", "country", "vendor", "product_group",
        "line_item_quantity", "line_item_value"
    ).show(3, truncate=25)


def main() -> None:
    """Punto de entrada principal."""
    print("=" * 60)
    print("[bronze] INICIO - Ingesta Suppy_Chain_Shipment_Data.csv")
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
