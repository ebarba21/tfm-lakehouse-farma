"""
ConstrucciÃ³n de la capa Gold: Modelo Dimensional (Estrella).

Dimensiones: dim_time, dim_product, dim_customer, dim_sales_rep
Hechos: fact_sales (histÃ³rico), fact_orders (streaming)

El modelo permite anÃ¡lisis cruzado entre ventas histÃ³ricas y pedidos
en tiempo real gracias a la consistencia de datos en Bronze/Silver.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, monotonically_increasing_id, concat_ws, md5,
    year, month, dayofmonth, quarter, dayofweek, weekofyear,
    date_format, row_number, first, when, create_map,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    min as spark_min, max as spark_max,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType, StructType, StructField, StringType,
    DoubleType, TimestampType
)
from pyspark.sql.utils import AnalysisException
from itertools import chain


# --- Rutas ---

SILVER_PHARMA_PATH = "/data/silver/pharma_sales"
SILVER_ORDERS_PATH = "/data/silver/orders"

GOLD_PATH = "/data/gold"
DIM_TIME_PATH = f"{GOLD_PATH}/dim_time"
DIM_PRODUCT_PATH = f"{GOLD_PATH}/dim_product"
DIM_CUSTOMER_PATH = f"{GOLD_PATH}/dim_customer"
DIM_SALES_REP_PATH = f"{GOLD_PATH}/dim_sales_rep"
FACT_SALES_PATH = f"{GOLD_PATH}/fact_sales"
FACT_ORDERS_PATH = f"{GOLD_PATH}/fact_orders"

# Esquema mínimo de Silver orders para poder trabajar sin datos de streaming
EMPTY_ORDERS_SCHEMA = StructType([
    StructField("event_id",      StringType(),    True),
    StructField("order_id",      StringType(),    True),
    StructField("event_ts",      TimestampType(), True),
    StructField("customer_name", StringType(),    True),
    StructField("city",          StringType(),    True),
    StructField("country",       StringType(),    True),
    StructField("channel",       StringType(),    True),
    StructField("sub_channel",   StringType(),    True),
    StructField("product_name",  StringType(),    True),
    StructField("product_class", StringType(),    True),
    StructField("qty",           IntegerType(),   True),
    StructField("unit_price",    DoubleType(),    True),
    StructField("currency",      StringType(),    True),
    StructField("latitude",      DoubleType(),    True),
    StructField("longitude",     DoubleType(),    True),
    StructField("total_amount",  DoubleType(),    True),
])


def load_silver_safe(spark, path, empty_schema, label):
    """Carga una tabla Silver Delta de forma segura.
    Si la ruta no existe o está vacía devuelve un DataFrame vacío con el
    esquema correcto, evitando que el pipeline completo falle por la
    ausencia de datos de streaming."""
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        if count == 0:
            print(f"[gold] WARN: {label} existe pero está vacío. "
                  "Se usará DataFrame vacío (sin datos de streaming).")
            return spark.createDataFrame([], empty_schema)
        print(f"  - {label}: {count:,} registros")
        return df
    except AnalysisException:
        print(f"[gold] WARN: {label} no encontrado en {path}. "
              "Se usará DataFrame vacío (consumer de Kafka no ha corrido).")
        return spark.createDataFrame([], empty_schema)


def create_spark_session():
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


def write_delta(df, path, table_name):
    count = df.count()
    print(f"[gold] Escribiendo {table_name}: {count:,} registros â†’ {path}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    return count


# --- Dimensiones ---

def build_dim_time(spark, pharma_df, orders_df):
    """Dimensión temporal a partir de las fechas en pharma + orders."""
    print("\n[gold] Construyendo DIM_TIME...")

    pharma_dates = (
        pharma_df
        .select(col("year").cast(IntegerType()), col("month"))
        .distinct()
    )

    # Solo se añaden fechas de orders si hay datos de streaming
    if orders_df.rdd.isEmpty():
        print("[gold] INFO: orders_df vacío, DIM_TIME se construye solo desde pharma_sales.")
        all_dates = pharma_dates
    else:
        orders_dates = (
            orders_df
            .select(
                year("event_ts").alias("year").cast(IntegerType()),
                date_format("event_ts", "MMMM").alias("month")
            )
            .distinct()
        )
        all_dates = pharma_dates.union(orders_dates).distinct()

    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }
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
            "time_sk", "year", "month", "month_num",
            "quarter", "quarter_name", "year_month"
        )
    )

    write_delta(dim_time, DIM_TIME_PATH, "dim_time")
    return dim_time


def build_dim_product(spark, pharma_df):
    """DimensiÃ³n de productos con surrogate key."""
    print("\n[gold] Construyendo DIM_PRODUCT...")

    products = (
        pharma_df
        .select("product_name", "product_class")
        .distinct()
        .withColumn("product_sk", row_number().over(Window.orderBy("product_name")))
        .withColumn("product_nk", md5(col("product_name")))
        .select("product_sk", "product_nk", "product_name", "product_class")
    )

    write_delta(products, DIM_PRODUCT_PATH, "dim_product")
    return products


def build_dim_customer(spark, pharma_df):
    """DimensiÃ³n de clientes con info geogrÃ¡fica y de canal."""
    print("\n[gold] Construyendo DIM_CUSTOMER...")

    # first() para obtener valor representativo cuando hay variaciÃ³n
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
        .withColumn("customer_sk", row_number().over(Window.orderBy("customer_name")))
        .withColumn("customer_nk", md5(col("customer_name")))
        .select(
            "customer_sk", "customer_nk", "customer_name",
            "city", "country", "latitude", "longitude",
            "channel", "sub_channel", "distributor"
        )
    )

    write_delta(customers, DIM_CUSTOMER_PATH, "dim_customer")
    return customers


def build_dim_sales_rep(spark, pharma_df):
    """DimensiÃ³n de representantes: rep â†’ manager â†’ team."""
    print("\n[gold] Construyendo DIM_SALES_REP...")

    sales_reps = (
        pharma_df
        .select("name_of_sales_rep", "manager", "sales_team")
        .distinct()
        .filter(col("name_of_sales_rep").isNotNull())
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


# --- Hechos ---

def build_fact_sales(spark, pharma_df, dim_time, dim_product, dim_customer, dim_sales_rep):
    """Tabla de hechos de ventas histÃ³ricas. Granularidad: 1 fila por transacciÃ³n."""
    print("\n[gold] Construyendo FACT_SALES...")

    dim_time_join = dim_time.select(
        col("time_sk"),
        col("year").alias("t_year"),
        col("month").alias("t_month")
    )

    fact = (
        pharma_df
        .join(
            dim_time_join,
            (pharma_df.year == dim_time_join.t_year) &
            (pharma_df.month == dim_time_join.t_month),
            "left"
        )
        .join(dim_product.select("product_sk", "product_name"), "product_name", "left")
        .join(dim_customer.select("customer_sk", "customer_name"), "customer_name", "left")
        .join(
            dim_sales_rep.select("sales_rep_sk", col("sales_rep_name").alias("name_of_sales_rep")),
            "name_of_sales_rep", "left"
        )
        .withColumn("sale_sk", monotonically_increasing_id())
        .select(
            "sale_sk", "time_sk", "product_sk", "customer_sk", "sales_rep_sk",
            col("quantity").alias("qty"), "price", "sales",
            "channel", "sub_channel", "country", "city"
        )
    )

    write_delta(fact, FACT_SALES_PATH, "fact_sales")
    return fact


def build_fact_orders(spark, orders_df, dim_product, dim_customer):
    """Tabla de hechos de pedidos streaming. Granularidad: 1 fila por evento."""
    print("\n[gold] Construyendo FACT_ORDERS...")

    if orders_df.rdd.isEmpty():
        print("[gold] INFO: orders_df vacío. FACT_ORDERS se crea como tabla vacía.")
        empty_schema = StructType([
            StructField("order_sk",     IntegerType(), True),
            StructField("event_id",     StringType(),  True),
            StructField("order_id",     StringType(),  True),
            StructField("event_ts",     TimestampType(), True),
            StructField("product_sk",   IntegerType(), True),
            StructField("customer_sk",  IntegerType(), True),
            StructField("qty",          IntegerType(), True),
            StructField("unit_price",   DoubleType(),  True),
            StructField("total_amount", DoubleType(),  True),
            StructField("channel",      StringType(),  True),
            StructField("sub_channel",  StringType(),  True),
            StructField("country",      StringType(),  True),
            StructField("city",         StringType(),  True),
            StructField("currency",     StringType(),  True),
        ])
        write_delta(spark.createDataFrame([], empty_schema), FACT_ORDERS_PATH, "fact_orders")
        return spark.createDataFrame([], empty_schema)

    fact = (
        orders_df
        .join(dim_product.select("product_sk", "product_name"), "product_name", "left")
        .join(dim_customer.select("customer_sk", "customer_name"), "customer_name", "left")
        .withColumn("order_sk", monotonically_increasing_id())
        .select(
            "order_sk", "event_id", "order_id", "event_ts",
            "product_sk", "customer_sk",
            "qty", "unit_price", "total_amount",
            "channel", "sub_channel", "country", "city", "currency"
        )
    )

    write_delta(fact, FACT_ORDERS_PATH, "fact_orders")
    return fact


def main():
    print("=" * 70)
    print("[gold] INICIO - ConstrucciÃ³n del Modelo Dimensional")
    print("=" * 70)

    spark = create_spark_session()

    try:
        print("\n[gold] Cargando datos Silver...")
        pharma_df = spark.read.format("delta").load(SILVER_PHARMA_PATH)
        print(f"  - pharma_sales: {pharma_df.count():,} registros")
        orders_df = load_silver_safe(
            spark, SILVER_ORDERS_PATH, EMPTY_ORDERS_SCHEMA, "orders"
        )

        # Dimensiones
        dim_time = build_dim_time(spark, pharma_df, orders_df)
        dim_product = build_dim_product(spark, pharma_df)
        dim_customer = build_dim_customer(spark, pharma_df)
        dim_sales_rep = build_dim_sales_rep(spark, pharma_df)

        # Hechos
        build_fact_sales(spark, pharma_df, dim_time, dim_product, dim_customer, dim_sales_rep)
        build_fact_orders(spark, orders_df, dim_product, dim_customer)

        # Resumen
        print("\n" + "=" * 70)
        print("[gold] RESUMEN DEL MODELO DIMENSIONAL")
        print("=" * 70)
        print("\nDIMENSIONES:")
        for t in ["dim_time", "dim_product", "dim_customer", "dim_sales_rep"]:
            n = spark.read.format("delta").load(f"{GOLD_PATH}/{t}").count()
            print(f"  - {t}: {n:,} registros")

        print("\nHECHOS:")
        for t in ["fact_sales", "fact_orders"]:
            n = spark.read.format("delta").load(f"{GOLD_PATH}/{t}").count()
            print(f"  - {t}: {n:,} registros")

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