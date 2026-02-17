"""
Pronóstico de demanda por producto-territorio usando Spark MLlib.

Agrega ventas mensuales y entrena un GBTRegressor con features temporales
(lags, medias móviles, estacionalidad). Output: gold.ml_demand_forecast
"""

import math
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ML_Demand_Forecast")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_silver_data(spark):
    logger.info("Cargando datos de Silver...")
    return spark.read.format("delta").load("/data/silver/pharma_sales")


def prepare_monthly_aggregation(df):
    """Agrega ventas mensuales por producto y territorio (country)."""
    logger.info("Preparando agregación mensual por producto-territorio...")

    # Silver tiene 'month' como string ("January") y 'year' como int
    # Hay que convertir el nombre del mes a número
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
        "year_month",
        F.to_date(F.concat(F.col("year"), F.lit("-"),
                           F.lpad(F.col("month_num").cast("string"), 2, "0"),
                           F.lit("-01")))
    )

    monthly_sales = (
        df_with_date
        .groupBy("product_name", "country", "year", "month_num", "year_month")
        .agg(
            F.sum("sales").alias("total_sales"),
            F.sum("quantity").alias("total_quantity"),
            F.count("*").alias("num_transactions"),
            F.avg("price").alias("avg_price")
        )
        .withColumnRenamed("month_num", "month")
    )

    logger.info(f"Registros mensuales generados: {monthly_sales.count()}")
    return monthly_sales


def create_lag_features(df):
    """
    Crea lags y medias móviles para el forecast:
    lag_1, lag_2, lag_3, moving_avg_3, moving_avg_6,
    quarter, is_q4, month_sin, month_cos
    """
    logger.info("Creando features de lag y media móvil...")

    window_spec = Window.partitionBy("product_name", "country").orderBy("year_month")

    df_with_lags = (
        df
        .withColumn("lag_1", F.lag("total_sales", 1).over(window_spec))
        .withColumn("lag_2", F.lag("total_sales", 2).over(window_spec))
        .withColumn("lag_3", F.lag("total_sales", 3).over(window_spec))
    )

    window_3m = Window.partitionBy("product_name", "country").orderBy("year_month").rowsBetween(-2, 0)
    window_6m = Window.partitionBy("product_name", "country").orderBy("year_month").rowsBetween(-5, 0)

    df_with_features = (
        df_with_lags
        .withColumn("moving_avg_3", F.avg("total_sales").over(window_3m))
        .withColumn("moving_avg_6", F.avg("total_sales").over(window_6m))
    )

    # Features de estacionalidad
    df_final = (
        df_with_features
        .withColumn("quarter", F.quarter(F.col("year_month")))
        .withColumn("is_q4", F.when(F.quarter(F.col("year_month")) == 4, 1).otherwise(0))
        .withColumn("month_sin", F.sin(2 * math.pi * F.col("month") / 12))
        .withColumn("month_cos", F.cos(2 * math.pi * F.col("month") / 12))
    )

    # Primeros meses no tienen lags completos, los descartamos
    df_clean = df_final.filter(F.col("lag_3").isNotNull())

    logger.info(f"Registros después de crear lags: {df_clean.count()}")
    return df_clean


def train_forecast_model(df):
    """Entrena GBTRegressor y devuelve (modelo, predicciones, métricas)."""
    logger.info("Preparando pipeline de ML...")

    # Solo features numéricas (evitamos StringIndexer para product_name y country
    # porque tienen demasiados valores únicos: 241 productos, 8 países)
    feature_cols = [
        "year", "month", "quarter", "is_q4",
        "month_sin", "month_cos",
        "lag_1", "lag_2", "lag_3",
        "moving_avg_3", "moving_avg_6",
        "total_quantity", "avg_price"
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="skip"
    )

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="total_sales",
        predictionCol="predicted_sales",
        maxIter=50, maxDepth=5, maxBins=64, seed=42
    )

    pipeline = Pipeline(stages=[assembler, gbt])

    # Split temporal: últimos 6 meses como test
    max_date = df.agg(F.max("year_month")).collect()[0][0]
    split_date = df.agg(F.add_months(F.max("year_month"), -6)).collect()[0][0]

    train_df = df.filter(F.col("year_month") < split_date)
    test_df = df.filter(F.col("year_month") >= split_date)
    logger.info(f"Train size: {train_df.count()}, Test size: {test_df.count()}")

    logger.info("Entrenando modelo GBTRegressor...")
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    # Métricas
    rmse = RegressionEvaluator(labelCol="total_sales", predictionCol="predicted_sales",
                               metricName="rmse").evaluate(predictions)
    mae = RegressionEvaluator(labelCol="total_sales", predictionCol="predicted_sales",
                              metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol="total_sales", predictionCol="predicted_sales",
                             metricName="r2").evaluate(predictions)

    metrics = {
        "rmse": rmse, "mae": mae, "r2": r2,
        "train_size": train_df.count(), "test_size": test_df.count()
    }

    logger.info(f"Métricas del modelo:")
    logger.info(f"  RMSE: {rmse:,.2f}")
    logger.info(f"  MAE: {mae:,.2f}")
    logger.info(f"  R²: {r2:.4f}")

    return model, predictions, metrics


def generate_forecast_output(predictions, metrics, spark):
    """DataFrame final para Gold con predicciones, errores y métricas."""
    logger.info("Generando output de forecast...")

    forecast_df = (
        predictions.select(
            "product_name", "country", "year", "month",
            F.col("year_month").alias("forecast_date"),
            F.col("total_sales").alias("actual_sales"),
            "predicted_sales", "total_quantity", "num_transactions",
            "lag_1", "moving_avg_3"
        )
        .withColumn("absolute_error",
                     F.abs(F.col("actual_sales") - F.col("predicted_sales")))
        .withColumn("percentage_error",
                     F.when(F.col("actual_sales") > 0,
                            F.abs(F.col("actual_sales") - F.col("predicted_sales"))
                            / F.col("actual_sales") * 100)
                     .otherwise(0))
        .withColumn("forecast_accuracy",
                     F.when(F.col("actual_sales") > 0,
                            (1 - F.abs(F.col("actual_sales") - F.col("predicted_sales"))
                             / F.col("actual_sales")) * 100)
                     .otherwise(100))
    )

    # Añadir métricas del modelo como columnas
    forecast_final = (
        forecast_df
        .withColumn("model_rmse", F.lit(metrics["rmse"]))
        .withColumn("model_mae", F.lit(metrics["mae"]))
        .withColumn("model_r2", F.lit(metrics["r2"]))
        .withColumn("model_type", F.lit("GBTRegressor"))
        .withColumn("created_at", F.current_timestamp())
    )

    return forecast_final


def save_to_gold(df, path):
    logger.info(f"Guardando forecast en {path}...")
    df.write.format("delta").mode("overwrite").save(path)
    logger.info(f"Forecast guardado. Registros: {df.count()}")


def print_forecast_summary(df):
    print("\n" + "="*70)
    print("RESUMEN DE DEMAND FORECAST")
    print("="*70)

    stats = df.agg(
        F.count("*").alias("total_predictions"),
        F.countDistinct("product_name").alias("unique_products"),
        F.countDistinct("country").alias("unique_countries"),
        F.avg("forecast_accuracy").alias("avg_accuracy"),
        F.avg("percentage_error").alias("avg_error_pct"),
        F.first("model_rmse").alias("rmse"),
        F.first("model_r2").alias("r2")
    ).collect()[0]

    print(f"\n  Estadísticas del Modelo:")
    print(f"   Total predicciones: {stats['total_predictions']:,}")
    print(f"   Productos únicos: {stats['unique_products']}")
    print(f"   Países únicos: {stats['unique_countries']}")
    print(f"   RMSE: {stats['rmse']:,.2f}")
    print(f"   R²: {stats['r2']:.4f}")
    print(f"   Accuracy promedio: {stats['avg_accuracy']:.2f}%")
    print(f"   Error porcentual promedio: {stats['avg_error_pct']:.2f}%")

    print(f"\n  Top 5 Productos con Mejor Accuracy:")
    (
        df.groupBy("product_name")
        .agg(
            F.avg("forecast_accuracy").alias("avg_accuracy"),
            F.sum("actual_sales").alias("total_sales")
        )
        .orderBy(F.desc("avg_accuracy"))
        .show(5, truncate=False)
    )

    print(f"\n  Muestra de Predicciones:")
    (
        df.select(
            "product_name", "country", "forecast_date",
            F.round("actual_sales", 2).alias("actual"),
            F.round("predicted_sales", 2).alias("predicted"),
            F.round("forecast_accuracy", 2).alias("accuracy_%")
        )
        .orderBy(F.desc("forecast_date"))
        .show(10, truncate=False)
    )
    print("="*70 + "\n")


def main():
    logger.info("Iniciando ML Demand Forecast Pipeline...")
    spark = create_spark_session()

    try:
        silver_df = load_silver_data(spark)
        monthly_df = prepare_monthly_aggregation(silver_df)
        features_df = create_lag_features(monthly_df)
        model, predictions, metrics = train_forecast_model(features_df)
        forecast_df = generate_forecast_output(predictions, metrics, spark)
        save_to_gold(forecast_df, "/data/gold/ml_demand_forecast")
        print_forecast_summary(forecast_df)
        logger.info("ML Demand Forecast Pipeline completado!")
    except Exception as e:
        logger.error(f"Error en el pipeline: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()