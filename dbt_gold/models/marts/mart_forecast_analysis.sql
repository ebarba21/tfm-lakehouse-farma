{{
    config(
        materialized='table',
        file_format='parquet'
    )
}}

/*
    Mart: Forecast Accuracy Analysis
    ================================
    Analiza la precisión del modelo de forecast de demanda,
    comparando predicciones vs valores reales por producto y territorio.
    
    Métricas clave:
    - MAPE (Mean Absolute Percentage Error) por producto
    - Accuracy promedio por país
    - Productos con mejor/peor predicción
*/

WITH forecast_data AS (
    SELECT
        product_name,
        country,
        year,
        month,
        forecast_date,
        actual_sales,
        predicted_sales,
        absolute_error,
        percentage_error,
        forecast_accuracy,
        total_quantity,
        model_r2,
        model_rmse
    FROM {{ source('gold_ml', 'ml_demand_forecast') }}
),

product_accuracy AS (
    SELECT
        product_name,
        COUNT(*) AS num_predictions,
        ROUND(AVG(actual_sales), 2) AS avg_actual_sales,
        ROUND(AVG(predicted_sales), 2) AS avg_predicted_sales,
        ROUND(AVG(percentage_error), 2) AS mape,
        ROUND(AVG(forecast_accuracy), 2) AS avg_accuracy,
        ROUND(SUM(actual_sales), 2) AS total_actual_sales,
        ROUND(SUM(predicted_sales), 2) AS total_predicted_sales
    FROM forecast_data
    GROUP BY product_name
),

country_accuracy AS (
    SELECT
        country,
        COUNT(*) AS num_predictions,
        ROUND(AVG(forecast_accuracy), 2) AS avg_accuracy,
        ROUND(AVG(percentage_error), 2) AS mape
    FROM forecast_data
    GROUP BY country
)

SELECT
    pa.product_name,
    pa.num_predictions,
    pa.avg_actual_sales,
    pa.avg_predicted_sales,
    pa.mape AS product_mape,
    pa.avg_accuracy AS product_accuracy_pct,
    pa.total_actual_sales,
    pa.total_predicted_sales,
    -- Clasificación de precisión
    CASE 
        WHEN pa.avg_accuracy >= 90 THEN 'Excelente'
        WHEN pa.avg_accuracy >= 80 THEN 'Bueno'
        WHEN pa.avg_accuracy >= 70 THEN 'Aceptable'
        ELSE 'Necesita mejora'
    END AS accuracy_category,
    -- Métricas del modelo (constantes)
    fd.model_r2,
    fd.model_rmse
FROM product_accuracy pa
CROSS JOIN (SELECT DISTINCT model_r2, model_rmse FROM forecast_data LIMIT 1) fd
ORDER BY pa.avg_accuracy DESC