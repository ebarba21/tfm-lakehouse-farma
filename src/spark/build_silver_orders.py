"""
Transforma los datos crudos de Bronze (streaming de pedidos) a la capa Silver.

Transformaciones aplicadas:
- Conversión de event_ts de string a timestamp
- Normalización de texto (country en mayúsculas, channel en minúsculas)
- Limpieza de espacios en blanco
- Filtrado de registros con campos críticos nulos
- Deduplicación por event_id
- Cálculo de campo derivado: total_amount = qty * unit_price
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    upper,
    lower,
    trim,
    initcap,
    row_number,
)
from pyspark.sql.window import Window


# CONFIGURACIÓN
BRONZE_PATH: str = "/data/bronze/orders_stream_raw"
SILVER_PATH: str = "/data/silver/orders"


def create_spark_session() -> SparkSession:
    """Crea sesión Spark con soporte Delta Lake."""
    return (
        SparkSession.builder
        .appName("build_silver_orders")
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
    print(f"[silver] Columnas disponibles: {df.columns}")
    return df


def transform_to_silver(df):
    """
    Aplica transformaciones de limpieza y normalización.
    
    Maneja tanto el esquema antiguo (site_id, product_id) como el nuevo
    (customer_name, product_name) para compatibilidad.
    """
    print("[silver] Aplicando transformaciones...")
    
    # Detectar esquema (nuevo vs antiguo)
    columns = df.columns
    is_new_schema = "customer_name" in columns
    
    if is_new_schema:
        print("[silver] Detectado esquema NUEVO (consistente con pharma-data)")
        return transform_new_schema(df)
    else:
        print("[silver] Detectado esquema ANTIGUO (IDs genéricos)")
        return transform_old_schema(df)


def transform_new_schema(df):
    """Transformaciones para el nuevo esquema consistente con pharma."""
    
    # Paso 1: Limpieza y normalización
    cleaned = (
        df
        # Timestamp
        .withColumn("event_ts", to_timestamp(col("event_ts")))
        # IDs
        .withColumn("event_id", trim(col("event_id")))
        .withColumn("order_id", trim(col("order_id")))
        # Cliente/Site - Initcap para consistencia con Silver pharma
        .withColumn("customer_name", initcap(trim(col("customer_name"))))
        .withColumn("city", initcap(trim(col("city"))))
        # País - mayúsculas
        .withColumn("country", upper(trim(col("country"))))
        # Canal - minúsculas
        .withColumn("channel", lower(trim(col("channel"))))
        .withColumn("sub_channel", lower(trim(col("sub_channel"))))
        # Producto - Initcap
        .withColumn("product_name", initcap(trim(col("product_name"))))
        .withColumn("product_class", initcap(trim(col("product_class"))))
        # Moneda - mayúsculas
        .withColumn("currency", upper(trim(col("currency"))))
    )
    
    # Paso 2: Filtrar registros inválidos
    filtered = (
        cleaned
        .filter(col("event_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("event_ts").isNotNull())
        .filter(col("customer_name").isNotNull())
        .filter(col("product_name").isNotNull())
        .filter(col("qty").isNotNull())
        .filter(col("unit_price").isNotNull())
        .filter(col("qty") > 0)
        .filter(col("unit_price") >= 0)
    )
    
    # Paso 3: Deduplicación por event_id
    window_spec = Window.partitionBy("event_id").orderBy("event_ts")
    deduplicated = (
        filtered
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    
    # Paso 4: Campo derivado
    final = deduplicated.withColumn(
        "total_amount",
        col("qty") * col("unit_price")
    )
    
    return final


def transform_old_schema(df):
    """Transformaciones para el esquema antiguo (compatibilidad)."""
    
    cleaned = (
        df
        .withColumn("event_ts", to_timestamp(col("event_ts")))
        .withColumn("event_id", trim(col("event_id")))
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("site_id", trim(col("site_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("channel", lower(trim(col("channel"))))
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("currency", upper(trim(col("currency"))))
    )
    
    filtered = (
        cleaned
        .filter(col("event_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("event_ts").isNotNull())
        .filter(col("qty").isNotNull())
        .filter(col("unit_price").isNotNull())
        .filter(col("qty") > 0)
        .filter(col("unit_price") >= 0)
    )
    
    window_spec = Window.partitionBy("event_id").orderBy("event_ts")
    deduplicated = (
        filtered
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    
    final = deduplicated.withColumn(
        "total_amount",
        col("qty") * col("unit_price")
    )
    
    return final


def write_silver(df, path: str) -> int:
    """Escribe el DataFrame a Silver en formato Delta."""
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


def main() -> None:
    """Punto de entrada principal."""
    print("=" * 60)
    print("[silver] INICIO - Construcción de capa Silver (orders)")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        bronze_df = read_bronze(spark, BRONZE_PATH)
        silver_df = transform_to_silver(bronze_df)
        
        print("\n[silver] Esquema de la tabla Silver:")
        silver_df.printSchema()
        
        records_written = write_silver(silver_df, SILVER_PATH)
        
        print("\n[silver] Muestra de datos (5 registros):")
        spark.read.format("delta").load(SILVER_PATH).show(5, truncate=False)
        
        print("=" * 60)
        print(f"[silver] FIN - {records_written:,} registros procesados")
        print("=" * 60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()