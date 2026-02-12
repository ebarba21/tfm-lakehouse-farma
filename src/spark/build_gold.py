"""
Construcción de la capa Gold: Modelo Dimensional (Estrella).

Este script genera todas las tablas del modelo dimensional:

DIMENSIONES:
- dim_time: Dimensión temporal (año, mes, día, trimestre, etc.)
- dim_product: Catálogo de productos
- dim_customer: Clientes/puntos de venta
- dim_sales_rep: Representantes de ventas y estructura comercial

HECHOS:
- fact_sales: Ventas históricas (desde pharma_sales)
- fact_orders: Pedidos en tiempo real (desde orders streaming)

El modelo permite análisis cruzado entre ventas históricas y pedidos
en tiempo real gracias a la consistencia de datos lograda en las capas
Bronze y Silver.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    monotonically_increasing_id,
    concat_ws,
    md5,
    year,
    month,
    dayofmonth,
    quarter,
    dayofweek,
    weekofyear,
    date_format,
    row_number,
    first,
    sum as spark_sum,
    count as spark_count,
    avg as spark_avg,
    min as spark_min,
    max as spark_max,
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

# Rutas Silver (fuente)
SILVER_PHARMA_PATH: str = "/data/silver/pharma_sales"
SILVER_ORDERS_PATH: str = "/data/silver/orders"

# Rutas Gold (destino)
GOLD_PATH: str = "/data/gold"
DIM_TIME_PATH: str = f"{GOLD_PATH}/dim_time"
DIM_PRODUCT_PATH: str = f"{GOLD_PATH}/dim_product"
DIM_CUSTOMER_PATH: str = f"{GOLD_PATH}/dim_customer"
DIM_SALES_REP_PATH: str = f"{GOLD_PATH}/dim_sales_rep"
FACT_SALES_PATH: str = f"{GOLD_PATH}/fact_sales"
FACT_ORDERS_PATH: str = f"{GOLD_PATH}/fact_orders"


def create_spark_session() -> SparkSession:
    """Crea sesión Spark con soporte Delta Lake."""
    return (
        SparkSession.builder
        .appName("build_gold_dimensional")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def write_delta(df: DataFrame, path: str, table_name: str) -> int:
    """Escribe DataFrame a Delta Lake."""
    count = df.count()
    print(f"[gold] Escribiendo {table_name}: {count:,} registros → {path}")
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    
    return count


# ==============================================================================
# DIMENSIONES
# ==============================================================================

def build_dim_time(spark: SparkSession, pharma_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """
    Construye dimensión temporal a partir de las fechas en los datos.
    
    Genera una fila por cada combinación año-mes presente en los datos,
    con atributos útiles para análisis temporal.
    """
    print("\n[gold] === Construyendo DIM_TIME ===")
    
    # Extraer fechas únicas de pharma (año, mes)
    pharma_dates = (
        pharma_df
        .select(
            col("year").cast(IntegerType()),
            col("month")
        )
        .distinct()
    )
    
    # Extraer fechas únicas de orders (desde timestamp)
    orders_dates = (
        orders_df
        .select(
            year("event_ts").alias("year").cast(IntegerType()),
            date_format("event_ts", "MMMM").alias("month")
        )
        .distinct()
    )
    
    # Unir todas las fechas
    all_dates = pharma_dates.union(orders_dates).distinct()
    
    # Mapeo de mes a número
    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }
    
    # Crear dimensión con atributos
    from pyspark.sql.functions import when, create_map
    from itertools import chain
    
    # Crear mapeo como expresión Spark
    mapping_expr = create_map([lit(x) for x in chain(*month_map.items())])
    
    dim_time = (
        all_dates
        .withColumn("month_num", mapping_expr[col("month")])
        .withColumn("quarter", ((col("month_num") - 1) / 3 + 1).cast(IntegerType()))
        .withColumn("quarter_name", concat_ws("", lit("Q"), col("quarter")))
        .withColumn("year_month", concat_ws("-", col("year"), col("month_num")))
        .withColumn(
            "time_sk",
            row_number().over(Window.orderBy("year", "month_num"))
        )
        .select(
            "time_sk",
            "year",
            "month",
            "month_num",
            "quarter",
            "quarter_name",
            "year_month"
        )
    )
    
    write_delta(dim_time, DIM_TIME_PATH, "dim_time")
    return dim_time


def build_dim_product(spark: SparkSession, pharma_df: DataFrame) -> DataFrame:
    """
    Construye dimensión de productos desde pharma_sales.
    
    Cada producto único tiene una surrogate key y sus atributos.
    """
    print("\n[gold] === Construyendo DIM_PRODUCT ===")
    
    # Extraer productos únicos con sus atributos
    products = (
        pharma_df
        .select("product_name", "product_class")
        .distinct()
        # Generar surrogate key
        .withColumn(
            "product_sk",
            row_number().over(Window.orderBy("product_name"))
        )
        # Generar clave natural (hash para joins)
        .withColumn(
            "product_nk",
            md5(col("product_name"))
        )
        .select(
            "product_sk",
            "product_nk",
            "product_name",
            "product_class"
        )
    )
    
    write_delta(products, DIM_PRODUCT_PATH, "dim_product")
    return products


def build_dim_customer(spark: SparkSession, pharma_df: DataFrame) -> DataFrame:
    """
    Construye dimensión de clientes/puntos de venta.
    
    Incluye información geográfica y de canal.
    """
    print("\n[gold] === Construyendo DIM_CUSTOMER ===")
    
    # Extraer clientes únicos con sus atributos
    # Usamos first() para obtener un valor representativo cuando hay variación
    customers = (
        pharma_df
        .groupBy("customer_name")
        .agg(
            first("city").alias("city"),
            first("country").alias("country"),
            first("latitude").alias("latitude"),
            first("longitude").alias("longitude"),
            first("channel").alias("channel"),
            first("sub_channel").alias("sub_channel"),
            first("distributor").alias("distributor"),
        )
        # Generar surrogate key
        .withColumn(
            "customer_sk",
            row_number().over(Window.orderBy("customer_name"))
        )
        # Generar clave natural
        .withColumn(
            "customer_nk",
            md5(col("customer_name"))
        )
        .select(
            "customer_sk",
            "customer_nk",
            "customer_name",
            "city",
            "country",
            "latitude",
            "longitude",
            "channel",
            "sub_channel",
            "distributor"
        )
    )
    
    write_delta(customers, DIM_CUSTOMER_PATH, "dim_customer")
    return customers


def build_dim_sales_rep(spark: SparkSession, pharma_df: DataFrame) -> DataFrame:
    """
    Construye dimensión de representantes de ventas.
    
    Incluye jerarquía comercial: rep → manager → team.
    """
    print("\n[gold] === Construyendo DIM_SALES_REP ===")
    
    # Extraer sales reps únicos con su jerarquía
    sales_reps = (
        pharma_df
        .select("name_of_sales_rep", "manager", "sales_team")
        .distinct()
        .filter(col("name_of_sales_rep").isNotNull())
        # Generar surrogate key
        .withColumn(
            "sales_rep_sk",
            row_number().over(Window.orderBy("name_of_sales_rep"))
        )
        .select(
            "sales_rep_sk",
            col("name_of_sales_rep").alias("sales_rep_name"),
            "manager",
            "sales_team"
        )
    )
    
    write_delta(sales_reps, DIM_SALES_REP_PATH, "dim_sales_rep")
    return sales_reps


# ==============================================================================
# HECHOS
# ==============================================================================

def build_fact_sales(
    spark: SparkSession,
    pharma_df: DataFrame,
    dim_time: DataFrame,
    dim_product: DataFrame,
    dim_customer: DataFrame,
    dim_sales_rep: DataFrame
) -> DataFrame:
    """
    Construye tabla de hechos de ventas históricas.
    
    Granularidad: una fila por transacción de venta.
    Métricas: quantity, price, sales (amount).
    """
    print("\n[gold] === Construyendo FACT_SALES ===")
    
    # Preparar dim_time para join (necesitamos year + month)
    dim_time_join = dim_time.select(
        col("time_sk"),
        col("year").alias("t_year"),
        col("month").alias("t_month")
    )
    
    # Join con dimensiones para obtener surrogate keys
    fact = (
        pharma_df
        # Join con dim_time
        .join(
            dim_time_join,
            (pharma_df.year == dim_time_join.t_year) & 
            (pharma_df.month == dim_time_join.t_month),
            "left"
        )
        # Join con dim_product
        .join(
            dim_product.select("product_sk", "product_name"),
            "product_name",
            "left"
        )
        # Join con dim_customer
        .join(
            dim_customer.select("customer_sk", "customer_name"),
            "customer_name",
            "left"
        )
        # Join con dim_sales_rep
        .join(
            dim_sales_rep.select("sales_rep_sk", col("sales_rep_name").alias("name_of_sales_rep")),
            "name_of_sales_rep",
            "left"
        )
        # Generar clave de hecho
        .withColumn(
            "sale_sk",
            monotonically_increasing_id()
        )
        # Seleccionar columnas finales
        .select(
            "sale_sk",
            "time_sk",
            "product_sk",
            "customer_sk",
            "sales_rep_sk",
            # Métricas
            col("quantity").alias("qty"),
            "price",
            "sales",
            # Atributos degenerados (útiles para análisis sin join)
            "channel",
            "sub_channel",
            "country",
            "city"
        )
    )
    
    write_delta(fact, FACT_SALES_PATH, "fact_sales")
    return fact


def build_fact_orders(
    spark: SparkSession,
    orders_df: DataFrame,
    dim_product: DataFrame,
    dim_customer: DataFrame
) -> DataFrame:
    """
    Construye tabla de hechos de pedidos en tiempo real.
    
    Granularidad: una fila por evento de pedido.
    Métricas: qty, unit_price, total_amount.
    """
    print("\n[gold] === Construyendo FACT_ORDERS ===")
    
    # Join con dimensiones
    fact = (
        orders_df
        # Join con dim_product
        .join(
            dim_product.select("product_sk", "product_name"),
            "product_name",
            "left"
        )
        # Join con dim_customer
        .join(
            dim_customer.select("customer_sk", "customer_name"),
            "customer_name",
            "left"
        )
        # Generar clave de hecho
        .withColumn(
            "order_sk",
            monotonically_increasing_id()
        )
        # Seleccionar columnas finales
        .select(
            "order_sk",
            "event_id",
            "order_id",
            "event_ts",
            "product_sk",
            "customer_sk",
            # Métricas
            "qty",
            "unit_price",
            "total_amount",
            # Atributos degenerados
            "channel",
            "sub_channel",
            "country",
            "city",
            "currency"
        )
    )
    
    write_delta(fact, FACT_ORDERS_PATH, "fact_orders")
    return fact


# ==============================================================================
# MAIN
# ==============================================================================

def main() -> None:
    """Punto de entrada principal - construye todo el modelo Gold."""
    
    print("=" * 70)
    print("[gold] INICIO - Construcción del Modelo Dimensional")
    print("=" * 70)
    
    spark = create_spark_session()
    
    try:
        # ---------------------------------------------------------------------
        # Cargar datos Silver
        # ---------------------------------------------------------------------
        print("\n[gold] Cargando datos Silver...")
        
        pharma_df = spark.read.format("delta").load(SILVER_PHARMA_PATH)
        orders_df = spark.read.format("delta").load(SILVER_ORDERS_PATH)
        
        print(f"  - pharma_sales: {pharma_df.count():,} registros")
        print(f"  - orders: {orders_df.count():,} registros")
        
        # ---------------------------------------------------------------------
        # Construir DIMENSIONES
        # ---------------------------------------------------------------------
        dim_time = build_dim_time(spark, pharma_df, orders_df)
        dim_product = build_dim_product(spark, pharma_df)
        dim_customer = build_dim_customer(spark, pharma_df)
        dim_sales_rep = build_dim_sales_rep(spark, pharma_df)
        
        # ---------------------------------------------------------------------
        # Construir HECHOS
        # ---------------------------------------------------------------------
        fact_sales = build_fact_sales(
            spark, pharma_df, dim_time, dim_product, dim_customer, dim_sales_rep
        )
        fact_orders = build_fact_orders(
            spark, orders_df, dim_product, dim_customer
        )
        
        # ---------------------------------------------------------------------
        # Resumen final
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("[gold] RESUMEN DEL MODELO DIMENSIONAL")
        print("=" * 70)
        
        print("\nDIMENSIONES:")
        print(f"  - dim_time:      {spark.read.format('delta').load(DIM_TIME_PATH).count():,} registros")
        print(f"  - dim_product:   {spark.read.format('delta').load(DIM_PRODUCT_PATH).count():,} registros")
        print(f"  - dim_customer:  {spark.read.format('delta').load(DIM_CUSTOMER_PATH).count():,} registros")
        print(f"  - dim_sales_rep: {spark.read.format('delta').load(DIM_SALES_REP_PATH).count():,} registros")
        
        print("\nHECHOS:")
        print(f"  - fact_sales:  {spark.read.format('delta').load(FACT_SALES_PATH).count():,} registros")
        print(f"  - fact_orders: {spark.read.format('delta').load(FACT_ORDERS_PATH).count():,} registros")
        
        # Mostrar muestras
        print("\n[gold] Muestra FACT_SALES:")
        spark.read.format("delta").load(FACT_SALES_PATH).show(5, truncate=False)
        
        print("\n[gold] Muestra FACT_ORDERS:")
        spark.read.format("delta").load(FACT_ORDERS_PATH).show(5, truncate=False)
        
        print("\n" + "=" * 70)
        print("[gold] FIN - Modelo dimensional construido exitosamente")
        print("=" * 70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()