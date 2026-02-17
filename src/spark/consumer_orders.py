"""
Consumer de streaming: ingesta de pedidos desde Kafka a Delta Lake Bronze.

Esquema consistente con pharma-data.csv para permitir JOINs en Gold
entre ventas históricas (fact_sales) y pedidos en tiempo real (fact_orders)
usando las mismas dimensiones (dim_product, dim_customer).
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

# Esquema consistente con pharma-data.csv
ORDER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    # Cliente
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("sub_channel", StringType(), True),
    # Producto
    StructField("product_name", StringType(), True),
    StructField("product_class", StringType(), True),
    # Transacción
    StructField("qty", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("currency", StringType(), True),
    # Coordenadas
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])


def create_spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka_stream(spark, bootstrap_servers, topic):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_order_events(raw_df, schema):
    """Parsea JSON de Kafka según el esquema de pedidos."""
    return raw_df.select(
        from_json(col("value").cast("string"), schema).alias("v")
    ).select("v.*")


def write_to_delta_bronze(df, output_path, checkpoint_path):
    """Escribe streaming a Delta con checkpointing (exactly-once)."""
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )


def main():
    output_path = "/data/bronze/orders_stream_raw"
    checkpoint_path = "/data/checkpoints/orders_stream_raw"

    spark = create_spark_session("orders_stream_to_delta_bronze")

    print(f"[consumer] Conectando a Kafka: {KAFKA_BOOTSTRAP}")
    print(f"[consumer] Topic: {TOPIC}")
    print(f"[consumer] Output: {output_path}")
    print(f"[consumer] Checkpoint: {checkpoint_path}")

    raw = read_kafka_stream(spark, KAFKA_BOOTSTRAP, TOPIC)
    parsed = parse_order_events(raw, ORDER_EVENT_SCHEMA)

    print("[consumer] Iniciando streaming query...")
    query = write_to_delta_bronze(parsed, output_path, checkpoint_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()