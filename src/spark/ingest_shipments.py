"""
Ingesta batch de Suppy_Chain_Shipment_Data.csv a Bronze (Delta Lake).

NOTA: Todas las columnas se leen como STRING para preservar
valores problemáticos (fechas con texto, etc.) que se limpiarán en Silver.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


LANDING_PATH = "/data/landing/Suppy_Chain_Shipment_Data.csv"
BRONZE_PATH = "/data/bronze/shipments_raw"


def create_spark_session():
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


def normalize_column_name(name):
    """snake_case y quita caracteres especiales."""
    import re
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[^\w\s]", "", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned


def ingest_to_bronze(spark):
    print(f"[bronze] Leyendo CSV desde: {LANDING_PATH}")

    # Todo como string para preservar datos problemáticos
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(LANDING_PATH)
    )

    for old_name in df.columns:
        new_name = normalize_column_name(old_name)
        df = df.withColumnRenamed(old_name, new_name)

    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("Suppy_Chain_Shipment_Data.csv"))
    )

    record_count = df.count()
    print(f"[bronze] Registros leídos: {record_count:,}")

    print("\n[bronze] Esquema:")
    df.printSchema()
    print(f"\n[bronze] Total columnas: {len(df.columns)}")

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


def validate_bronze(spark):
    print("\n[bronze] Validando tabla...")
    df = spark.read.format("delta").load(BRONZE_PATH)
    total = df.count()

    # Comprobar valores problemáticos en columnas de fecha
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

    print("\n[bronze] Muestra (3 registros):")
    df.select(
        "id", "country", "vendor", "product_group",
        "line_item_quantity", "line_item_value"
    ).show(3, truncate=25)


def main():
    print("=" * 60)
    print("[bronze] INICIO - Ingesta shipments")
    print("=" * 60)

    spark = create_spark_session()
    try:
        ingest_to_bronze(spark)
        validate_bronze(spark)
        print("=" * 60)
        print("[bronze] FIN - Ingesta completada")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()