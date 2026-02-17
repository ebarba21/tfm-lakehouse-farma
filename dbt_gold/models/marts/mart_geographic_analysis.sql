-- Análisis de ventas por geografía (país/ciudad)

{{ config(materialized='table') }}

select
    c.country,
    c.city,

    -- Volumen
    count(distinct c.customer_sk) as total_customers,
    count(distinct f.sales_rep_sk) as sales_reps_active,
    count(*) as total_transactions,

    -- Ventas
    round(sum(f.sales), 2) as total_sales,
    round(avg(f.sales), 2) as avg_transaction_value,
    sum(f.qty) as total_units_sold,

    count(distinct f.product_sk) as products_sold,

    -- Derivadas
    round(sum(f.sales) / nullif(count(distinct c.customer_sk), 0), 2) as sales_per_customer,
    round(sum(f.sales) / nullif(count(*), 0), 2) as avg_ticket,

    -- Coordenadas promedio para mapas
    round(avg(c.latitude), 6) as avg_latitude,
    round(avg(c.longitude), 6) as avg_longitude

from {{ source('gold', 'fact_sales') }} f
inner join {{ source('gold', 'dim_customer') }} c
    on f.customer_sk = c.customer_sk

group by
    c.country,
    c.city

order by total_sales desc