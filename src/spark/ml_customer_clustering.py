#!/usr/bin/env python3
"""
ML Customer Clustering - TFM Lakehouse Farma
Clustering de clientes basado en comportamiento de compra.

LÓGICA DE ETIQUETADO:
- Se calculan estadísticas por cluster (avg_sales, avg_transactions, etc.)
- Se RANKEA cada cluster por avg_sales
- El cluster con MAYOR avg_sales = "Premium - Alto Volumen"
- El cluster con avg_sales MEDIO = "Regular - Volumen Medio"
- El cluster con MENOR avg_sales = "Básico - Bajo Volumen"

Cada cluster tiene UNA ÚNICA etiqueta.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    lit, row_number
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime

def main():
    print("\n" + "="*70)
    print("ML CUSTOMER CLUSTERING - TFM Lakehouse Farma (v2 - Etiquetado Corregido)")
    print("="*70 + "\n")
    
    # Crear SparkSession
    spark = SparkSession.builder \
        .appName("ML_Customer_Clustering_v2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # =========================================================================
    # 1. CARGAR DATOS DE SILVER
    # =========================================================================
    print("[1/7] Cargando datos de silver.pharma_sales...")
    
    silver_sales = spark.read.format("delta").load("/data/silver/pharma_sales")
    total_records = silver_sales.count()
    print(f"      Registros en silver: {total_records:,}")
    
    # =========================================================================
    # 2. FEATURE ENGINEERING - Agregar por cliente
    # =========================================================================
    print("\n[2/7] Creando features por cliente...")
    
    customer_features = silver_sales.groupBy("customer_name") \
        .agg(
            spark_sum("sales").alias("total_sales"),
            spark_sum("quantity").alias("total_qty"),
            count("*").alias("transaction_count"),
            spark_round(avg("sales"), 2).alias("avg_transaction_value"),
            avg("latitude").alias("latitude"),
            avg("longitude").alias("longitude")
        ) \
        .filter(col("total_sales").isNotNull() & (col("total_sales") > 0))
    
    num_customers = customer_features.count()
    print(f"      Clientes únicos con ventas > 0: {num_customers}")
    
    # Mostrar distribución de features
    print("\n      Distribución de features:")
    customer_features.select("total_sales", "transaction_count", "avg_transaction_value") \
        .summary("min", "25%", "50%", "75%", "max") \
        .show()
    
    # =========================================================================
    # 3. PREPARAR FEATURES PARA ML
    # =========================================================================
    print("[3/7] Preparando features para ML (VectorAssembler + StandardScaler)...")
    
    feature_cols = ["total_sales", "total_qty", "transaction_count", "avg_transaction_value"]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    assembled = assembler.transform(customer_features)
    
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(assembled)
    scaled_data = scaler_model.transform(assembled)
    
    print(f"      Features utilizadas: {feature_cols}")
    
    # =========================================================================
    # 4. ENTRENAR K-MEANS (K=3)
    # =========================================================================
    print("\n[4/7] Entrenando modelo K-Means con K=3...")
    
    kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster_id")
    model = kmeans.fit(scaled_data)
    
    predictions = model.transform(scaled_data)
    
    # Evaluar calidad del clustering
    evaluator = ClusteringEvaluator(predictionCol="cluster_id", featuresCol="features")
    silhouette = evaluator.evaluate(predictions)
    print(f"      Silhouette Score: {silhouette:.4f}")
    print("      (Valores cercanos a 1 indican clusters bien separados)")
    
    # =========================================================================
    # 5. CALCULAR ESTADÍSTICAS POR CLUSTER
    # =========================================================================
    print("\n[5/7] Calculando estadísticas por cluster...")
    
    cluster_stats = predictions.groupBy("cluster_id") \
        .agg(
            count("*").alias("num_clientes"),
            spark_round(avg("total_sales"), 2).alias("avg_sales"),
            spark_round(avg("transaction_count"), 2).alias("avg_transactions"),
            spark_round(avg("avg_transaction_value"), 2).alias("avg_ticket"),
            spark_round(spark_sum("total_sales"), 2).alias("sum_sales")
        )
    
    print("\n      === ESTADÍSTICAS POR CLUSTER (antes de etiquetar) ===")
    cluster_stats.orderBy("cluster_id").show()
    
    # =========================================================================
    # 6. ASIGNAR ETIQUETAS BASADAS EN RANKING DE AVG_SALES
    # =========================================================================
    print("[6/7] Asignando etiquetas basadas en ranking de avg_sales...")
    
    # Crear ranking: 1 = menor avg_sales, 3 = mayor avg_sales
    window_spec = Window.orderBy(col("avg_sales").asc())
    
    cluster_stats_ranked = cluster_stats.withColumn(
        "sales_rank", 
        row_number().over(window_spec)
    )
    
    # Asignar etiqueta según ranking
    # rank 1 (menor avg_sales) = Básico
    # rank 2 (medio) = Regular
    # rank 3 (mayor avg_sales) = Premium
    cluster_labels = cluster_stats_ranked.withColumn(
        "cluster_label",
        lit(None).cast("string")
    ).withColumn(
        "cluster_label",
        lit("").cast("string")
    )
    
    # Usar when/otherwise para asignar etiquetas
    from pyspark.sql.functions import when
    
    cluster_labels = cluster_stats_ranked.withColumn(
        "cluster_label",
        when(col("sales_rank") == 1, lit("Básico - Bajo Volumen"))
        .when(col("sales_rank") == 2, lit("Regular - Volumen Medio"))
        .when(col("sales_rank") == 3, lit("Premium - Alto Volumen"))
    )
    
    print("\n      === MAPEO CLUSTER -> ETIQUETA ===")
    cluster_labels.select(
        "cluster_id", 
        "cluster_label", 
        "num_clientes",
        "avg_sales",
        "avg_transactions",
        "avg_ticket",
        "sales_rank"
    ).orderBy("sales_rank").show(truncate=False)
    
    # Extraer solo cluster_id y cluster_label para el join
    label_mapping = cluster_labels.select("cluster_id", "cluster_label")
    
    # =========================================================================
    # 7. GUARDAR EN GOLD CON MÉTRICAS DETALLADAS
    # =========================================================================
    print("[7/7] Guardando en gold/ml_customer_clusters...")
    
    # Unir predicciones con etiquetas
    final_output = predictions \
        .join(label_mapping, "cluster_id", "left") \
        .select(
            col("customer_name").alias("customer_id"),
            col("cluster_id"),
            col("cluster_label"),
            spark_round(col("total_sales"), 2).alias("total_sales"),
            col("total_qty"),
            col("transaction_count"),
            spark_round(col("avg_transaction_value"), 2).alias("avg_transaction_value"),
            spark_round(col("latitude"), 6).alias("latitude"),
            spark_round(col("longitude"), 6).alias("longitude"),
            lit(datetime.now().isoformat()).alias("created_at")
        )
    
    # Guardar (overwrite para reemplazar datos anteriores)
    final_output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/data/gold/ml_customer_clusters")
    
    # =========================================================================
    # VERIFICACIÓN FINAL
    # =========================================================================
    print("\n" + "="*70)
    print("VERIFICACIÓN FINAL")
    print("="*70)
    
    result = spark.read.format("delta").load("/data/gold/ml_customer_clusters")
    
    print(f"\nTotal registros guardados: {result.count()}")
    
    print("\n=== DISTRIBUCIÓN POR CLUSTER ===")
    result.groupBy("cluster_id", "cluster_label") \
        .agg(
            count("*").alias("num_clientes"),
            spark_round(avg("total_sales"), 2).alias("avg_sales"),
            spark_round(avg("transaction_count"), 2).alias("avg_transactions")
        ) \
        .orderBy("cluster_id") \
        .show(truncate=False)
    
    print("\n=== CARACTERÍSTICAS DE CADA CLUSTER ===")
    for row in result.select("cluster_id", "cluster_label").distinct().orderBy("cluster_id").collect():
        cid = row["cluster_id"]
        label = row["cluster_label"]
        
        cluster_data = result.filter(col("cluster_id") == cid)
        stats = cluster_data.agg(
            count("*").alias("n"),
            spark_round(avg("total_sales"), 2).alias("avg_sales"),
            spark_round(avg("transaction_count"), 2).alias("avg_trans"),
            spark_round(avg("avg_transaction_value"), 2).alias("avg_ticket")
        ).collect()[0]
        
        print(f"\nCluster {cid}: {label}")
        print(f"  - Clientes: {stats['n']}")
        print(f"  - Ventas promedio: €{stats['avg_sales']:,.2f}")
        print(f"  - Transacciones promedio: {stats['avg_trans']:.1f}")
        print(f"  - Ticket promedio: €{stats['avg_ticket']:,.2f}")
    
    spark.stop()
    
    print("\n" + "="*70)
    print("ML CLUSTERING COMPLETADO EXITOSAMENTE")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()