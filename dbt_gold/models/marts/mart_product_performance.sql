-- mart_product_performance.sql
-- Análisis de rendimiento por producto
-- Combina fact_sales con dim_product y dim_time

{{ config(materialized='table') }}

select
    -- Dimensiones de producto
    p.product_sk,
    p.product_name,
    p.product_class,
    
    -- Métricas de volumen
    count(distinct f.customer_sk) as customers_reached,
    count(distinct f.sales_rep_sk) as sales_reps_selling,
    count(*) as total_transactions,
    
    -- Métricas de ventas
    round(sum(f.sales), 2) as total_sales,
    round(avg(f.sales), 2) as avg_transaction_value,
    sum(f.qty) as total_units_sold,
    round(avg(f.price), 2) as avg_price,
    
    -- Métricas de distribución geográfica
    count(distinct f.country) as countries_sold,
    count(distinct f.city) as cities_sold,
    
    -- Métricas derivadas
    round(sum(f.sales) / nullif(sum(f.qty), 0), 2) as revenue_per_unit,
    round(sum(f.sales) / nullif(count(distinct f.customer_sk), 0), 2) as sales_per_customer

from {{ source('gold', 'fact_sales') }} f
inner join {{ source('gold', 'dim_product') }} p
    on f.product_sk = p.product_sk

group by 
    p.product_sk,
    p.product_name,
    p.product_class

order by total_sales desc