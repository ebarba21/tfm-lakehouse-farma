{{
    config(
        materialized='table',
        file_format='parquet'
    )
}}

-- Análisis agregado de los segmentos de clientes generados por K-Means.
-- Segmentos: Premium (alto volumen), Regular (medio), Básico (bajo)

WITH cluster_data AS (
    SELECT
        customer_id,
        cluster_id,
        cluster_label,
        total_sales,
        total_qty,
        transaction_count,
        avg_transaction_value,
        latitude,
        longitude,
        created_at
    FROM {{ source('gold_ml', 'ml_customer_clusters') }}
),

cluster_summary AS (
    SELECT
        cluster_label,
        COUNT(*) AS num_customers,
        ROUND(AVG(total_sales), 2) AS avg_sales_per_customer,
        ROUND(SUM(total_sales), 2) AS total_cluster_sales,
        ROUND(AVG(transaction_count), 2) AS avg_transactions,
        ROUND(AVG(avg_transaction_value), 2) AS avg_ticket,
        ROUND(AVG(total_qty), 2) AS avg_quantity,
        ROUND(MIN(total_sales), 2) AS min_sales,
        ROUND(MAX(total_sales), 2) AS max_sales
    FROM cluster_data
    GROUP BY cluster_label
)

SELECT
    cs.cluster_label,
    cs.num_customers,
    cs.avg_sales_per_customer,
    cs.total_cluster_sales,
    cs.avg_transactions,
    cs.avg_ticket,
    cs.avg_quantity,
    cs.min_sales,
    cs.max_sales,
    ROUND(cs.total_cluster_sales * 100.0 / SUM(cs.total_cluster_sales) OVER(), 2) AS pct_of_total_sales,
    ROUND(cs.num_customers * 100.0 / SUM(cs.num_customers) OVER(), 2) AS pct_of_total_customers
FROM cluster_summary cs
ORDER BY cs.avg_sales_per_customer DESC