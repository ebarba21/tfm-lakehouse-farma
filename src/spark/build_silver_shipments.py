"""
Transforma los datos de Bronze (shipments_raw) a la capa Silver.

Este dataset tiene complejidades especiales:
- Fechas con valores no-fecha ("Pre-PQ Process", "Date Not Captured")
- Muchas columnas (33+)
- Caracteres especiales en países (Côte d'Ivoire)

Transformaciones aplicadas:
- Parseo robusto de fechas (valores no-fecha → null)
- Tipado numérico para cantidades, valores, pesos, costes
- Normalización de texto
- Filtrado de registros críticos
- Selección de columnas relevantes para el modelo dimensional
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    initcap,
    regexp_replace,
    to_date,
    when,
    coalesce,
)
from pyspark.sql.types import IntegerType, DoubleType


# CONFIGURACIÓN
BRONZE_PATH: str = "/data/bronze/shipments_raw"
SILVER_PATH: str = "/data/silver/shipments"


def create_spark_session() -> SparkSession:
    """Crea sesión Spark con soporte Delta Lake."""
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


def read_bronze(spark: SparkSession, path: str):
    """Lee la tabla Delta de Bronze."""
    print(f"[silver] Leyendo Bronze desde: {path}")
    df = spark.read.format("delta").load(path)
    record_count = df.count()
    print(f"[silver] Registros en Bronze: {record_count:,}")
    print(f"[silver] Columnas: {len(df.columns)}")
    return df


def parse_date_robust(df, col_name: str, new_col_name: str = None):
    """
    Parsea una columna de fecha de forma robusta.
    
    Valores como "Pre-PQ Process", "Date Not Captured" → null
    Fechas válidas como "2-Jun-06" → date
    
    Args:
        df: DataFrame
        col_name: Nombre de la columna original
        new_col_name: Nombre de la nueva columna (opcional)
    """
    if new_col_name is None:
        new_col_name = col_name
    
    # Intentar parsear con formato d-MMM-yy (ej: "2-Jun-06")
    return df.withColumn(
        new_col_name,
        when(
            # Si contiene texto no-fecha, poner null
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
    """
    Aplica todas las transformaciones de limpieza y normalización.
    """
    print("[silver] Aplicando transformaciones...")
    
    # =========================================
    # PASO 1: Seleccionar y renombrar columnas relevantes
    # =========================================
    # No necesitamos todas las 33+ columnas para el modelo dimensional
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
    
    # =========================================
    # PASO 2: Parseo robusto de fechas
    # =========================================
    with_dates = selected
    date_columns = [
        "scheduled_delivery_date",
        "delivered_to_client_date", 
        "delivery_recorded_date"
    ]
    
    for date_col in date_columns:
        with_dates = parse_date_robust(with_dates, date_col)
    
    # =========================================
    # PASO 3: Tipado numérico
    # =========================================
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
    
    # =========================================
    # PASO 4: Normalización de texto
    # =========================================
    normalized = (
        typed
        # País: limpiar y mayúsculas
        .withColumn("country", upper(trim(col("country"))))
        # Vendor: Initcap
        .withColumn("vendor", initcap(trim(col("vendor"))))
        # Product group: Initcap  
        .withColumn("product_group", initcap(trim(col("product_group"))))
        # Shipment mode: minúsculas
        .withColumn("shipment_mode", lower(trim(col("shipment_mode"))))
        # Brand: Initcap
        .withColumn("brand", initcap(trim(col("brand"))))
        # Fulfill via: minúsculas
        .withColumn("fulfill_via", lower(trim(col("fulfill_via"))))
    )
    
    # =========================================
    # PASO 5: Filtrado de registros inválidos
    # =========================================
    filtered = (
        normalized
        .filter(col("shipment_id").isNotNull())
        .filter(col("country").isNotNull())
        .filter(col("line_item_quantity").isNotNull())
        .filter(col("line_item_quantity") > 0)
    )
    
    records_after_filter = filtered.count()
    print(f"[silver] Registros después de filtrar: {records_after_filter:,}")
    
    # =========================================
    # PASO 6: Calcular campos derivados
    # =========================================
    final = (
        filtered
        # Días de entrega (si tenemos ambas fechas)
        .withColumn(
            "delivery_days",
            when(
                col("delivered_to_client_date").isNotNull() & 
                col("scheduled_delivery_date").isNotNull(),
                (col("delivered_to_client_date").cast("long") - 
                 col("scheduled_delivery_date").cast("long")) / 86400
            ).otherwise(None).cast(IntegerType())
        )
        # Flag de entrega tardía
        .withColumn(
            "is_late_delivery",
            when(col("delivery_days") > 0, True)
            .when(col("delivery_days") <= 0, False)
            .otherwise(None)
        )
    )
    
    return final


def write_silver(df, path: str) -> int:
    """Escribe el DataFrame transformado a Silver en formato Delta."""
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


def show_quality_metrics(spark: SparkSession, path: str) -> None:
    """Muestra métricas de calidad del dataset Silver."""
    print("\n[silver] === MÉTRICAS DE CALIDAD ===")
    
    df = spark.read.format("delta").load(path)
    
    total = df.count()
    
    # Conteos de fechas válidas
    valid_scheduled = df.filter(col("scheduled_delivery_date").isNotNull()).count()
    valid_delivered = df.filter(col("delivered_to_client_date").isNotNull()).count()
    late_deliveries = df.filter(col("is_late_delivery") == True).count()
    
    print(f"  Total registros: {total:,}")
    print(f"  Con fecha programada válida: {valid_scheduled:,} ({100*valid_scheduled/total:.1f}%)")
    print(f"  Con fecha entrega válida: {valid_delivered:,} ({100*valid_delivered/total:.1f}%)")
    print(f"  Entregas tardías: {late_deliveries:,}")
    
    # Distribución por modo de envío
    print("\n  Distribución por modo de envío:")
    df.groupBy("shipment_mode").count().orderBy(col("count").desc()).show()
    
    # Top 5 países por cantidad
    print("  Top 5 países por cantidad de envíos:")
    (
        df.groupBy("country")
        .count()
        .orderBy(col("count").desc())
        .limit(5)
        .show()
    )
    
    # Top product groups
    print("  Top 5 grupos de producto:")
    (
        df.groupBy("product_group")
        .count()
        .orderBy(col("count").desc())
        .limit(5)
        .show()
    )


def main() -> None:
    """Punto de entrada principal del job Silver shipments."""
    print("=" * 60)
    print("[silver] INICIO - Construcción de capa Silver (shipments)")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Leer Bronze
        bronze_df = read_bronze(spark, BRONZE_PATH)
        
        # Transformar
        silver_df = transform_to_silver(bronze_df)
        
        # Mostrar esquema
        print("\n[silver] Esquema de la tabla Silver:")
        silver_df.printSchema()
        
        # Escribir
        records_written = write_silver(silver_df, SILVER_PATH)
        
        # Métricas de calidad
        show_quality_metrics(spark, SILVER_PATH)
        
        # Muestra final
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
