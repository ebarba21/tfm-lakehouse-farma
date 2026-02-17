"""
Transformación Bronze -> Silver para pharma_sales.

Limpieza: tipado, normalización de texto, filtrado de nulos/inválidos,
deduplicación por clave natural (distributor+customer+product+city+country+month+year).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, initcap,
    regexp_replace, row_number, when, coalesce, lit,
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType


BRONZE_PATH = "/data/bronze/pharma_sales_raw"
SILVER_PATH = "/data/silver/pharma_sales"


def create_spark_session():
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


def read_bronze(spark, path):
    print(f"[silver] Leyendo Bronze desde: {path}")
    df = spark.read.format("delta").load(path)
    record_count = df.count()
    print(f"[silver] Registros en Bronze: {record_count:,}")
    return df


def clean_text_column(df, col_name, style="initcap"):
    """Limpia columna de texto: trim + espacios múltiples + capitalización."""
    cleaned = trim(regexp_replace(col(col_name), r"\s+", " "))

    if style == "upper":
        return df.withColumn(col_name, upper(cleaned))
    elif style == "lower":
        return df.withColumn(col_name, lower(cleaned))
    else:
        return df.withColumn(col_name, initcap(cleaned))


def transform_to_silver(df):
    """Aplica limpieza, tipado, filtrado y deduplicación."""
    print("[silver] Aplicando transformaciones...")

    # -- Limpieza de texto --
    cleaned = df
    # Nombres propios: Primera Letra Mayúscula
    for c in ["distributor", "customer_name", "city", "product_name",
              "name_of_sales_rep", "manager", "sales_team"]:
        cleaned = clean_text_column(cleaned, c, "initcap")

    cleaned = clean_text_column(cleaned, "country", "upper")
    cleaned = clean_text_column(cleaned, "channel", "lower")
    cleaned = clean_text_column(cleaned, "sub_channel", "lower")
    cleaned = clean_text_column(cleaned, "product_class", "initcap")

    # -- Tipado numérico --
    typed = (
        cleaned
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("sales", col("sales").cast(DoubleType()))
        .withColumn("latitude", col("latitude").cast(DoubleType()))
        .withColumn("longitude", col("longitude").cast(DoubleType()))
        .withColumn("year", col("year").cast(IntegerType()))
    )

    # -- Filtrado: campos críticos no nulos + reglas de negocio --
    filtered = (
        typed
        .filter(col("distributor").isNotNull())
        .filter(col("customer_name").isNotNull())
        .filter(col("product_name").isNotNull())
        .filter(col("quantity").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("country").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("price") >= 0)
        .filter(col("sales") >= 0)
    )

    records_after_filter = filtered.count()
    print(f"[silver] Registros después de filtrar: {records_after_filter:,}")

    # -- Deduplicación por clave natural --
    # Si hay duplicados, nos quedamos con el de mayor venta
    window_spec = (
        Window
        .partitionBy(
            "distributor", "customer_name", "product_name",
            "city", "country", "month", "year"
        )
        .orderBy(col("sales").desc())
    )

    deduplicated = (
        filtered
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    records_after_dedup = deduplicated.count()
    print(f"[silver] Registros después de deduplicar: {records_after_dedup:,}")

    # Quitar columnas de metadatos de Bronze
    final = deduplicated.drop("_ingested_at", "_source_file")
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
    """Métricas de calidad del dataset Silver."""
    print("\n[silver] === MÉTRICAS DE CALIDAD ===")
    df = spark.read.format("delta").load(path)

    total = df.count()
    distinct_products = df.select("product_name").distinct().count()
    distinct_customers = df.select("customer_name").distinct().count()
    distinct_countries = df.select("country").distinct().count()

    print(f"  Total registros: {total:,}")
    print(f"  Productos únicos: {distinct_products:,}")
    print(f"  Clientes únicos: {distinct_customers:,}")
    print(f"  Países: {distinct_countries:,}")

    print("\n  Distribución por canal:")
    df.groupBy("channel").count().orderBy(col("count").desc()).show()

    print("  Distribución por año:")
    df.groupBy("year").count().orderBy("year").show()

    print("  Top 5 países por ventas totales:")
    (
        df.groupBy("country")
        .agg({"sales": "sum"})
        .withColumnRenamed("sum(sales)", "total_sales")
        .orderBy(col("total_sales").desc())
        .limit(5)
        .show()
    )


def main():
    print("=" * 60)
    print("[silver] INICIO - Construcción de capa Silver (pharma_sales)")
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
        spark.read.format("delta").load(SILVER_PATH).show(5, truncate=25)

        print("=" * 60)
        print(f"[silver] FIN - {records_written:,} registros procesados")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()