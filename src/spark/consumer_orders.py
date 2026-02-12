"""
Consumer de streaming para ingesta de pedidos desde Kafka a Delta Lake Bronze.

ACTUALIZADO: Esquema consistente con pharma-data.csv para permitir JOINs
en la capa Gold entre ventas históricas y pedidos en tiempo real.

Este módulo implementa un job de Spark Structured Streaming que consume eventos
de pedidos desde un topic de Kafka, los parsea según el esquema definido y los
persiste en formato Delta Lake en la capa Bronze del lakehouse.

El consumer está diseñado para ejecutarse de forma continua, con soporte para
checkpointing que garantiza exactly-once semantics y tolerancia a fallos.

Ejemplo de uso:
    $ spark-submit consumer_orders.py

Variables de entorno:
    KAFKA_BOOTSTRAP_SERVERS: Dirección del broker de Kafka (default: kafka:9093)
    KAFKA_TOPIC: Nombre del topic a consumir (default: orders)
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
TOPIC: str = os.getenv("KAFKA_TOPIC", "orders")

# Esquema NUEVO consistente con pharma-data.csv
ORDER_EVENT_SCHEMA: StructType = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    # Datos del cliente (consistentes con pharma-data)
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("sub_channel", StringType(), True),
    # Datos del producto (consistentes con pharma-data)
    StructField("product_name", StringType(), True),
    StructField("product_class", StringType(), True),
    # Datos de la transacción
    StructField("qty", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("currency", StringType(), True),
    # Coordenadas geográficas
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])
"""
Esquema del evento de pedido CONSISTENTE con pharma-data.csv.

Esto permite hacer JOINs en Gold entre:
- fact_sales (ventas históricas de pharma_sales)
- fact_orders_stream (pedidos en tiempo real)

Usando las mismas dimensiones:
- dim_product (por product_name)
- dim_customer_site (por customer_name)
"""


def create_spark_session(app_name: str) -> SparkSession:
    """
    Crea y configura una sesión de Spark para streaming.

    Args:
        app_name: Nombre de la aplicación Spark para identificación en el UI.

    Returns:
        SparkSession: Sesión configurada con particiones optimizadas para
            el entorno local Docker.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    """
    Configura y retorna un DataFrame de streaming desde Kafka.

    Args:
        spark: Sesión de Spark activa.
        bootstrap_servers: Dirección del broker de Kafka (host:puerto).
        topic: Nombre del topic de Kafka a suscribirse.

    Returns:
        DataFrame: DataFrame de streaming con los datos crudos de Kafka,
            incluyendo columnas key, value, topic, partition, offset y timestamp.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_order_events(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parsea los eventos JSON de Kafka según el esquema de pedidos.

    Extrae el campo value del mensaje Kafka, lo interpreta como JSON
    usando el esquema proporcionado y expande las columnas resultantes.

    Args:
        raw_df: DataFrame de streaming con datos crudos de Kafka.
        schema: Esquema StructType que define la estructura del JSON.

    Returns:
        DataFrame: DataFrame con las columnas del evento parseadas y
            listas para persistencia.
    """
    return raw_df.select(
        from_json(col("value").cast("string"), schema).alias("v")
    ).select("v.*")


def write_to_delta_bronze(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
) -> StreamingQuery:
    """
    Escribe el DataFrame de streaming a Delta Lake en modo append.

    Configura el sink de Delta Lake con checkpointing para garantizar
    tolerancia a fallos y exactly-once semantics.

    Args:
        df: DataFrame de streaming a persistir.
        output_path: Ruta donde se escribirá la tabla Delta Bronze.
        checkpoint_path: Ruta para almacenar el estado del checkpoint,
            necesario para recuperación ante fallos.

    Returns:
        StreamingQuery: Query de streaming activa que puede ser monitoreada
            o detenida.
    """
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )


def main() -> None:
    """
    Punto de entrada principal del consumer de streaming.

    Orquesta el flujo completo: creación de sesión Spark, lectura de Kafka,
    parseo de eventos y escritura a Delta Lake. Bloquea indefinidamente
    esperando la terminación del stream.
    """
    output_path: str = "/data/bronze/orders_stream_raw"
    checkpoint_path: str = "/data/checkpoints/orders_stream_raw"

    spark: SparkSession = create_spark_session("orders_stream_to_delta_bronze")

    print(f"[consumer] Conectando a Kafka: {KAFKA_BOOTSTRAP}")
    print(f"[consumer] Topic: {TOPIC}")
    print(f"[consumer] Output: {output_path}")
    print(f"[consumer] Checkpoint: {checkpoint_path}")

    raw: DataFrame = read_kafka_stream(spark, KAFKA_BOOTSTRAP, TOPIC)
    parsed: DataFrame = parse_order_events(raw, ORDER_EVENT_SCHEMA)

    print("[consumer] Iniciando streaming query...")
    query: StreamingQuery = write_to_delta_bronze(parsed, output_path, checkpoint_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()