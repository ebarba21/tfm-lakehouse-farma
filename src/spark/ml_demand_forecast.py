"""
ML Demand Forecast - Pronóstico de Demanda por Producto-Territorio
===================================================================

Este script implementa un modelo de regresión para predecir ventas
mensuales por producto y territorio usando Spark MLlib.

Features:
- Agregación mensual de ventas
- Features temporales: mes, año, lag_1, lag_3, media_movil_3
- Modelo: GBTRegressor (Gradient Boosted Trees)
- Métricas: RMSE, MAE, R²

Output: gold.ml_demand_forecast

Autor: Eric
Proyecto: TFM Master Data Engineering - UCM
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Crea y configura la sesión de Spark con Delta Lake."""
    return (
        SparkSession.builder
        .appName("ML_Demand_Forecast")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_silver_data(spark: SparkSession) -> "DataFrame":
    """Carga datos de Silver pharma_sales."""
    logger.info("Cargando datos de Silver...")
    return spark.read.format("delta").load("/data/silver/pharma_sales")


def prepare_monthly_aggregation(df: "DataFrame") -> "DataFrame":
    """
    Agrega ventas mensuales por producto y territorio (country).
    
    Args:
        df: DataFrame de Silver pharma_sales
        
    Returns:
        DataFrame con agregación mensual
    """
    logger.info("Preparando agregación mensual por producto-territorio...")
    
    # Silver ya tiene 'month' (string como 'January') y 'year' (int)
    # Convertir month string a número
    month_map = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4,
        'May': 5, 'June': 6, 'July': 7, 'August': 8,
        'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    
    # Crear columna month_num desde el nombre del mes
    df_with_date = df.withColumn(
        "month_num",
        F.when(F.col("month") == "January", 1)
        .when(F.col("month") == "February", 2)
        .when(F.col("month") == "March", 3)
        .when(F.col("month") == "April", 4)
        .when(F.col("month") == "May", 5)
        .when(F.col("month") == "June", 6)
        .when(F.col("month") == "July", 7)
        .when(F.col("month") == "August", 8)
        .when(F.col("month") == "September", 9)
        .when(F.col("month") == "October", 10)
        .when(F.col("month") == "November", 11)
        .when(F.col("month") == "December", 12)
        .otherwise(1)
    ).withColumn(
        # Crear year_month como fecha para ordenar correctamente
        "year_month", 
        F.to_date(F.concat(F.col("year"), F.lit("-"), 
                          F.lpad(F.col("month_num").cast("string"), 2, "0"), 
                          F.lit("-01")))
    )
    
    # Agregar por producto, país, año y mes
    monthly_sales = df_with_date.groupBy(
        "product_name",
        "country",
        "year",
        "month_num",
        "year_month"
    ).agg(
        F.sum("sales").alias("total_sales"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("num_transactions"),
        F.avg("price").alias("avg_price")
    ).withColumnRenamed("month_num", "month")
    
    logger.info(f"Registros mensuales generados: {monthly_sales.count()}")
    return monthly_sales


def create_lag_features(df: "DataFrame") -> "DataFrame":
    """
    Crea features de lag y media móvil para el modelo de forecast.
    
    Features generadas:
    - lag_1: ventas del mes anterior
    - lag_2: ventas de hace 2 meses
    - lag_3: ventas de hace 3 meses
    - moving_avg_3: media móvil de 3 meses
    - moving_avg_6: media móvil de 6 meses
    """
    logger.info("Creando features de lag y media móvil...")
    
    # Definir ventana por producto-país ordenada por fecha
    window_spec = Window.partitionBy("product_name", "country").orderBy("year_month")
    
    # Crear lags
    df_with_lags = df.withColumn(
        "lag_1", F.lag("total_sales", 1).over(window_spec)
    ).withColumn(
        "lag_2", F.lag("total_sales", 2).over(window_spec)
    ).withColumn(
        "lag_3", F.lag("total_sales", 3).over(window_spec)
    )
    
    # Crear medias móviles
    window_3m = Window.partitionBy("product_name", "country").orderBy("year_month").rowsBetween(-2, 0)
    window_6m = Window.partitionBy("product_name", "country").orderBy("year_month").rowsBetween(-5, 0)
    
    df_with_features = df_with_lags.withColumn(
        "moving_avg_3", F.avg("total_sales").over(window_3m)
    ).withColumn(
        "moving_avg_6", F.avg("total_sales").over(window_6m)
    )
    
    # Features adicionales
    df_final = df_with_features.withColumn(
        "quarter", F.quarter(F.col("year_month"))
    ).withColumn(
        "is_q4", F.when(F.quarter(F.col("year_month")) == 4, 1).otherwise(0)
    ).withColumn(
        "month_sin", F.sin(2 * 3.14159 * F.col("month") / 12)
    ).withColumn(
        "month_cos", F.cos(2 * 3.14159 * F.col("month") / 12)
    )
    
    # Eliminar filas con nulls en features de lag (primeros meses)
    df_clean = df_final.filter(
        F.col("lag_3").isNotNull()
    )
    
    logger.info(f"Registros después de crear lags: {df_clean.count()}")
    return df_clean


def train_forecast_model(df: "DataFrame") -> tuple:
    """
    Entrena el modelo de forecast usando GBTRegressor.
    
    Returns:
        tuple: (modelo_entrenado, predicciones, métricas)
    """
    logger.info("Preparando pipeline de ML...")
    
    # NOTA: Evitamos StringIndexer para product_name y country porque tienen
    # demasiados valores únicos (241 productos, 8 países). En su lugar,
    # usamos solo features numéricas que ya capturan el comportamiento temporal.
    
    # Features numéricas para el modelo (sin categóricas de alta cardinalidad)
    feature_cols = [
        "year",
        "month",
        "quarter",
        "is_q4",
        "month_sin",
        "month_cos",
        "lag_1",
        "lag_2",
        "lag_3",
        "moving_avg_3",
        "moving_avg_6",
        "total_quantity",
        "avg_price"
    ]
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Modelo GBT (Gradient Boosted Trees) con maxBins aumentado por seguridad
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="total_sales",
        predictionCol="predicted_sales",
        maxIter=50,
        maxDepth=5,
        maxBins=64,  # Aumentado de 32 a 64
        seed=42
    )
    
    # Pipeline simplificado (sin StringIndexers)
    pipeline = Pipeline(stages=[
        assembler,
        gbt
    ])
    
    # Split train/test (80/20) - ordenado por tiempo
    # Usamos los últimos meses como test
    max_date = df.agg(F.max("year_month")).collect()[0][0]
    split_date = df.agg(
        F.add_months(F.max("year_month"), -6)
    ).collect()[0][0]
    
    train_df = df.filter(F.col("year_month") < split_date)
    test_df = df.filter(F.col("year_month") >= split_date)
    
    logger.info(f"Train size: {train_df.count()}, Test size: {test_df.count()}")
    
    # Entrenar modelo
    logger.info("Entrenando modelo GBTRegressor...")
    model = pipeline.fit(train_df)
    
    # Predicciones
    predictions = model.transform(test_df)
    
    # Evaluar modelo
    evaluator_rmse = RegressionEvaluator(
        labelCol="total_sales",
        predictionCol="predicted_sales",
        metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="total_sales",
        predictionCol="predicted_sales",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="total_sales",
        predictionCol="predicted_sales",
        metricName="r2"
    )
    
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    
    metrics = {
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "train_size": train_df.count(),
        "test_size": test_df.count()
    }
    
    logger.info(f"Métricas del modelo:")
    logger.info(f"  RMSE: {rmse:,.2f}")
    logger.info(f"  MAE: {mae:,.2f}")
    logger.info(f"  R²: {r2:.4f}")
    
    return model, predictions, metrics


def generate_forecast_output(
    predictions: "DataFrame",
    metrics: dict,
    spark: SparkSession
) -> "DataFrame":
    """
    Genera el DataFrame final para persistir en Gold.
    
    Incluye:
    - Predicciones vs valores reales
    - Error absoluto y porcentual
    - Métricas del modelo
    """
    logger.info("Generando output de forecast...")
    
    # Seleccionar columnas relevantes
    forecast_df = predictions.select(
        F.col("product_name"),
        F.col("country"),
        F.col("year"),
        F.col("month"),
        F.col("year_month").alias("forecast_date"),
        F.col("total_sales").alias("actual_sales"),
        F.col("predicted_sales"),
        F.col("total_quantity"),
        F.col("num_transactions"),
        F.col("lag_1"),
        F.col("moving_avg_3")
    ).withColumn(
        "absolute_error", F.abs(F.col("actual_sales") - F.col("predicted_sales"))
    ).withColumn(
        "percentage_error", 
        F.when(F.col("actual_sales") > 0,
               F.abs(F.col("actual_sales") - F.col("predicted_sales")) / F.col("actual_sales") * 100
        ).otherwise(0)
    ).withColumn(
        "forecast_accuracy",
        F.when(F.col("actual_sales") > 0,
               (1 - F.abs(F.col("actual_sales") - F.col("predicted_sales")) / F.col("actual_sales")) * 100
        ).otherwise(100)
    )
    
    # Añadir métricas del modelo como columnas
    forecast_final = forecast_df.withColumn(
        "model_rmse", F.lit(metrics["rmse"])
    ).withColumn(
        "model_mae", F.lit(metrics["mae"])
    ).withColumn(
        "model_r2", F.lit(metrics["r2"])
    ).withColumn(
        "model_type", F.lit("GBTRegressor")
    ).withColumn(
        "created_at", F.current_timestamp()
    )
    
    return forecast_final


def save_to_gold(df: "DataFrame", path: str) -> None:
    """Guarda el DataFrame en Gold como tabla Delta."""
    logger.info(f"Guardando forecast en {path}...")
    
    df.write.format("delta").mode("overwrite").save(path)
    
    logger.info(f"Forecast guardado exitosamente. Registros: {df.count()}")


def print_forecast_summary(df: "DataFrame") -> None:
    """Imprime resumen del forecast generado."""
    print("\n" + "="*70)
    print("RESUMEN DE DEMAND FORECAST")
    print("="*70)
    
    # Estadísticas generales
    stats = df.agg(
        F.count("*").alias("total_predictions"),
        F.countDistinct("product_name").alias("unique_products"),
        F.countDistinct("country").alias("unique_countries"),
        F.avg("forecast_accuracy").alias("avg_accuracy"),
        F.avg("percentage_error").alias("avg_error_pct"),
        F.first("model_rmse").alias("rmse"),
        F.first("model_r2").alias("r2")
    ).collect()[0]
    
    print(f"\n📊 Estadísticas del Modelo:")
    print(f"   Total predicciones: {stats['total_predictions']:,}")
    print(f"   Productos únicos: {stats['unique_products']}")
    print(f"   Países únicos: {stats['unique_countries']}")
    print(f"   RMSE: {stats['rmse']:,.2f}")
    print(f"   R²: {stats['r2']:.4f}")
    print(f"   Accuracy promedio: {stats['avg_accuracy']:.2f}%")
    print(f"   Error porcentual promedio: {stats['avg_error_pct']:.2f}%")
    
    # Top 5 productos con mejor forecast
    print(f"\n🏆 Top 5 Productos con Mejor Accuracy:")
    df.groupBy("product_name").agg(
        F.avg("forecast_accuracy").alias("avg_accuracy"),
        F.sum("actual_sales").alias("total_sales")
    ).orderBy(F.desc("avg_accuracy")).show(5, truncate=False)
    
    # Muestra de predicciones
    print(f"\n📋 Muestra de Predicciones:")
    df.select(
        "product_name",
        "country",
        "forecast_date",
        F.round("actual_sales", 2).alias("actual"),
        F.round("predicted_sales", 2).alias("predicted"),
        F.round("forecast_accuracy", 2).alias("accuracy_%")
    ).orderBy(F.desc("forecast_date")).show(10, truncate=False)
    
    print("="*70 + "\n")


def main():
    """Función principal del pipeline de forecast."""
    logger.info("Iniciando ML Demand Forecast Pipeline...")
    
    # Crear sesión Spark
    spark = create_spark_session()
    
    try:
        # 1. Cargar datos
        silver_df = load_silver_data(spark)
        
        # 2. Agregar por mes
        monthly_df = prepare_monthly_aggregation(silver_df)
        
        # 3. Crear features de lag
        features_df = create_lag_features(monthly_df)
        
        # 4. Entrenar modelo
        model, predictions, metrics = train_forecast_model(features_df)
        
        # 5. Generar output
        forecast_df = generate_forecast_output(predictions, metrics, spark)
        
        # 6. Guardar en Gold
        save_to_gold(forecast_df, "/data/gold/ml_demand_forecast")
        
        # 7. Imprimir resumen
        print_forecast_summary(forecast_df)
        
        logger.info("✅ ML Demand Forecast Pipeline completado exitosamente!")
        
    except Exception as e:
        logger.error(f"❌ Error en el pipeline: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()