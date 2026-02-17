"""
Transformación Bronze -> Silver para shipments.

Complejidades de este dataset:
- Fechas con valores no-fecha ("Pre-PQ Process", "Date Not Captured")
- 33+ columnas (seleccionamos las relevantes)
- Caracteres especiales en países (Côte d'Ivoire)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, initcap,
    regexp_replace, to_date, when, coalesce,
)
from pyspark.sql.types import IntegerType, DoubleType


BRONZE_PATH = "/data/bronze/shipments_raw"
SILVER_PATH = "/data/silver/shipments"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("build_silver_shipments")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_bronze(spark, path):
    print(f"[silver] Leyendo Bronze desde: {path}")
    df = spark.read.format("delta").load(path)
    record_count = df.count()
    print(f"[silver] Registros en Bronze: {record_count:,}")
    print(f"[silver] Columnas: {len(df.columns)}")
    return df


def parse_date_robust(df, col_name, new_col_name=None):
    """
    Parseo robusto de fecha. Valores como "Pre-PQ Process" o
    "Date Not Captured" se convierten a null. Fechas válidas
    tipo "2-Jun-06" se parsean con formato d-MMM-yy.
    """
    if new_col_name is None:
        new_col_name = col_name

    return df.withColumn(
        new_col_name,
        when(
            (col(col_name).contains("Process")) |
            (col(col_name).contains("Captured")) |
            (col(col_name).contains("Insufficient")) |
            (col(col_name).isNull()) |
            (trim(col(col_name)) == ""),
            None
        ).otherwise(
            to_date(col(col_name), "d-MMM-yy")
        )
    )


def transform_to_silver(df):
    print("[silver] Aplicando transformaciones...")

    # Seleccionar solo columnas relevantes (de las 33+)
    selected = df.select(
        col("id").alias("shipment_id"),
        col("project_code"),
        col("country"),
        col("managed_by"),
        col("fulfill_via"),
        col("vendor_inco_term"),
        col("shipment_mode"),
        col("scheduled_delivery_date"),
        col("delivered_to_client_date"),
        col("delivery_recorded_date"),
        col("product_group"),
        col("sub_classification"),
        col("vendor"),
        col("item_description"),
        col("molecule_test_type"),
        col("brand"),
        col("dosage"),
        col("dosage_form"),
        col("line_item_quantity"),
        col("line_item_value"),
        col("pack_price"),
        col("unit_price"),
        col("manufacturing_site"),
        col("weight_kilograms"),
        col("freight_cost_usd"),
        col("line_item_insurance_usd"),
    )

    # Parseo de fechas (robusto, valores de texto -> null)
    with_dates = selected
    for date_col in ["scheduled_delivery_date", "delivered_to_client_date",
                     "delivery_recorded_date"]:
        with_dates = parse_date_robust(with_dates, date_col)

    # Tipado numérico
    typed = (
        with_dates
        .withColumn("line_item_quantity", col("line_item_quantity").cast(IntegerType()))
        .withColumn("line_item_value", col("line_item_value").cast(DoubleType()))
        .withColumn("pack_price", col("pack_price").cast(DoubleType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("weight_kilograms", col("weight_kilograms").cast(DoubleType()))
        .withColumn("freight_cost_usd", col("freight_cost_usd").cast(DoubleType()))
        .withColumn("line_item_insurance_usd", col("line_item_insurance_usd").cast(DoubleType()))
    )

    # Normalización de texto
    normalized = (
        typed
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("vendor", initcap(trim(col("vendor"))))
        .withColumn("product_group", initcap(trim(col("product_group"))))
        .withColumn("shipment_mode", lower(trim(col("shipment_mode"))))
        .withColumn("brand", initcap(trim(col("brand"))))
        .withColumn("fulfill_via", lower(trim(col("fulfill_via"))))
    )

    # Filtrado
    filtered = (
        normalized
        .filter(col("shipment_id").isNotNull())
        .filter(col("country").isNotNull())
        .filter(col("line_item_quantity").isNotNull())
        .filter(col("line_item_quantity") > 0)
    )
    records_after_filter = filtered.count()
    print(f"[silver] Registros después de filtrar: {records_after_filter:,}")

    # Campos derivados: días de entrega y flag de retraso
    final = (
        filtered
        .withColumn(
            "delivery_days",
            when(
                col("delivered_to_client_date").isNotNull() &
                col("scheduled_delivery_date").isNotNull(),
                (col("delivered_to_client_date").cast("long") -
                 col("scheduled_delivery_date").cast("long")) / 86400
            ).otherwise(None).cast(IntegerType())
        )
        .withColumn(
            "is_late_delivery",
            when(col("delivery_days") > 0, True)
            .when(col("delivery_days") <= 0, False)
            .otherwise(None)
        )
    )

    return final


def write_silver(df, path):
    print(f"[silver] Escribiendo a: {path}")
    record_count = df.count()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    print(f"[silver] Registros escritos: {record_count:,}")
    return record_count


def show_quality_metrics(spark, path):
    print("\n[silver] === MÉTRICAS DE CALIDAD ===")
    df = spark.read.format("delta").load(path)
    total = df.count()

    valid_scheduled = df.filter(col("scheduled_delivery_date").isNotNull()).count()
    valid_delivered = df.filter(col("delivered_to_client_date").isNotNull()).count()
    late_deliveries = df.filter(col("is_late_delivery") == True).count()

    print(f"  Total registros: {total:,}")
    print(f"  Con fecha programada válida: {valid_scheduled:,} ({100*valid_scheduled/total:.1f}%)")
    print(f"  Con fecha entrega válida: {valid_delivered:,} ({100*valid_delivered/total:.1f}%)")
    print(f"  Entregas tardías: {late_deliveries:,}")

    print("\n  Distribución por modo de envío:")
    df.groupBy("shipment_mode").count().orderBy(col("count").desc()).show()

    print("  Top 5 países por cantidad de envíos:")
    df.groupBy("country").count().orderBy(col("count").desc()).limit(5).show()

    print("  Top 5 grupos de producto:")
    df.groupBy("product_group").count().orderBy(col("count").desc()).limit(5).show()


def main():
    print("=" * 60)
    print("[silver] INICIO - Construcción de capa Silver (shipments)")
    print("=" * 60)

    spark = create_spark_session()

    try:
        bronze_df = read_bronze(spark, BRONZE_PATH)
        silver_df = transform_to_silver(bronze_df)

        print("\n[silver] Esquema de la tabla Silver:")
        silver_df.printSchema()

        records_written = write_silver(silver_df, SILVER_PATH)
        show_quality_metrics(spark, SILVER_PATH)

        print("\n[silver] Muestra de datos (5 registros):")
        (
            spark.read.format("delta").load(SILVER_PATH)
            .select("shipment_id", "country", "product_group", "shipment_mode",
                    "line_item_quantity", "scheduled_delivery_date", "is_late_delivery")
            .show(5, truncate=20)
        )

        print("=" * 60)
        print(f"[silver] FIN - {records_written:,} registros procesados")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()