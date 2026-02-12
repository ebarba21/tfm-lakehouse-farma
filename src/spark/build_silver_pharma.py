"""
Transforma los datos de Bronze (pharma_sales_raw) a la capa Silver.

Transformaciones aplicadas:
- Tipado correcto de columnas numéricas (quantity, price, sales)
- Normalización de texto (trim, capitalización consistente)
- Limpieza de espacios extra en nombres
- Filtrado de registros con campos críticos nulos o inválidos
- Deduplicación por clave natural
- Validación de rangos (quantity > 0, price >= 0)

Dataset principal del TFM: ventas farmacéuticas por canal y territorio.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    initcap,
    regexp_replace,
    row_number,
    when,
    coalesce,
    lit,
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType


# CONFIGURACIÓN
BRONZE_PATH: str = "/data/bronze/pharma_sales_raw"
SILVER_PATH: str = "/data/silver/pharma_sales"


def create_spark_session() -> SparkSession:
    """Crea sesión Spark con soporte Delta Lake."""
    return (
        SparkSession.builder
        .appName("build_silver_pharma")
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
    return df


def clean_text_column(df, col_name: str, style: str = "initcap"):
    """
    Limpia una columna de texto: trim + espacios múltiples + capitalización.
    
    Args:
        df: DataFrame
        col_name: Nombre de la columna
        style: 'initcap', 'upper', o 'lower'
    """
    # Eliminar espacios al inicio/final y reducir espacios múltiples a uno
    cleaned = trim(regexp_replace(col(col_name), r"\s+", " "))
    
    if style == "upper":
        return df.withColumn(col_name, upper(cleaned))
    elif style == "lower":
        return df.withColumn(col_name, lower(cleaned))
    else:  # initcap
        return df.withColumn(col_name, initcap(cleaned))


def transform_to_silver(df):
    """
    Aplica todas las transformaciones de limpieza y normalización.
    
    Transformaciones:
    1. Limpieza de texto (trim, espacios múltiples)
    2. Normalización de capitalización
    3. Tipado numérico
    4. Filtrado de nulos y valores inválidos
    5. Deduplicación por clave natural
    """
    print("[silver] Aplicando transformaciones...")
    
    # =========================================
    # PASO 1: Limpieza de columnas de texto
    # =========================================
    cleaned = df
    
    # Nombres propios: Initcap (Primera Letra Mayúscula)
    for col_name in ["distributor", "customer_name", "city", "product_name", "name_of_sales_rep", "manager", "sales_team"]:
        cleaned = clean_text_column(cleaned, col_name, "initcap")
    
    # País: MAYÚSCULAS
    cleaned = clean_text_column(cleaned, "country", "upper")
    
    # Canal y subcanal: minúsculas
    cleaned = clean_text_column(cleaned, "channel", "lower")
    cleaned = clean_text_column(cleaned, "sub_channel", "lower")
    
    # Product class: Initcap
    cleaned = clean_text_column(cleaned, "product_class", "initcap")
    
    # =========================================
    # PASO 2: Tipado numérico
    # =========================================
    typed = (
        cleaned
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("sales", col("sales").cast(DoubleType()))
        .withColumn("latitude", col("latitude").cast(DoubleType()))
        .withColumn("longitude", col("longitude").cast(DoubleType()))
        .withColumn("year", col("year").cast(IntegerType()))
    )
    
    # =========================================
    # PASO 3: Filtrado de registros inválidos
    # =========================================
    # Campos críticos no pueden ser nulos
    filtered = (
        typed
        .filter(col("distributor").isNotNull())
        .filter(col("customer_name").isNotNull())
        .filter(col("product_name").isNotNull())
        .filter(col("quantity").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("country").isNotNull())
        # Reglas de negocio
        .filter(col("quantity") > 0)
        .filter(col("price") >= 0)
        .filter(col("sales") >= 0)
    )
    
    records_after_filter = filtered.count()
    print(f"[silver] Registros después de filtrar: {records_after_filter:,}")
    
    # =========================================
    # PASO 4: Deduplicación
    # =========================================
    # Clave natural: combinación que identifica una venta única
    # distributor + customer + product + city + country + month + year
    window_spec = (
        Window
        .partitionBy(
            "distributor", "customer_name", "product_name",
            "city", "country", "month", "year"
        )
        .orderBy(col("sales").desc())  # Si hay duplicados, quedamos con el de mayor venta
    )
    
    deduplicated = (
        filtered
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    
    records_after_dedup = deduplicated.count()
    print(f"[silver] Registros después de deduplicar: {records_after_dedup:,}")
    
    # =========================================
    # PASO 5: Eliminar columnas de metadatos de Bronze
    # =========================================
    final = deduplicated.drop("_ingested_at", "_source_file")
    
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
    
    # Conteos básicos
    total = df.count()
    distinct_products = df.select("product_name").distinct().count()
    distinct_customers = df.select("customer_name").distinct().count()
    distinct_countries = df.select("country").distinct().count()
    
    print(f"  Total registros: {total:,}")
    print(f"  Productos únicos: {distinct_products:,}")
    print(f"  Clientes únicos: {distinct_customers:,}")
    print(f"  Países: {distinct_countries:,}")
    
    # Distribución por canal
    print("\n  Distribución por canal:")
    df.groupBy("channel").count().orderBy(col("count").desc()).show()
    
    # Distribución por año
    print("  Distribución por año:")
    df.groupBy("year").count().orderBy("year").show()
    
    # Top 5 países por ventas
    print("  Top 5 países por ventas totales:")
    (
        df.groupBy("country")
        .agg({"sales": "sum"})
        .withColumnRenamed("sum(sales)", "total_sales")
        .orderBy(col("total_sales").desc())
        .limit(5)
        .show()
    )


def main() -> None:
    """Punto de entrada principal del job Silver pharma."""
    print("=" * 60)
    print("[silver] INICIO - Construcción de capa Silver (pharma_sales)")
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
        spark.read.format("delta").load(SILVER_PATH).show(5, truncate=25)
        
        print("=" * 60)
        print(f"[silver] FIN - {records_written:,} registros procesados")
        print("=" * 60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
