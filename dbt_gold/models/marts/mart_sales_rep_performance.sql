-- mart_sales_rep_performance.sql
-- Análisis de rendimiento por representante comercial
-- Usa el modelo dimensional Gold (fact_sales + dim_sales_rep)

{{ config(materialized='table') }}

select
    -- Atributos de la dimensión (fuente de verdad)
    d.sales_rep_sk,
    d.sales_rep_name,
    d.manager,
    d.sales_team,
    
    -- Métricas calculadas desde la fact
    count(distinct f.customer_sk) as total_customers,
    count(*) as total_transactions,
    round(sum(f.sales), 2) as total_sales,
    round(avg(f.sales), 2) as avg_transaction_value,
    sum(f.qty) as total_units_sold,
    round(sum(f.sales) / nullif(count(distinct f.customer_sk), 0), 2) as sales_per_customer,
    count(distinct f.product_sk) as products_sold,
    count(distinct f.city) as cities_covered

from {{ source('gold', 'fact_sales') }} f
inner join {{ source('gold', 'dim_sales_rep') }} d
    on f.sales_rep_sk = d.sales_rep_sk

group by 
    d.sales_rep_sk,
    d.sales_rep_name,
    d.manager,
    d.sales_team

order by total_sales desc