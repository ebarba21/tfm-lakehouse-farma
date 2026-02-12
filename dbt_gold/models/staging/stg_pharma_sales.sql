-- stg_pharma_sales.sql
-- Modelo staging que lee de la capa Silver de pharma_sales
-- Sirve como fuente de verdad para los marts

{{ config(materialized='view') }}

select
    customer_name,
    city,
    country,
    latitude,
    longitude,
    channel,
    sub_channel,
    product_name,
    product_class,
    quantity,
    price,
    sales,
    month,
    year,
    distributor,
    name_of_sales_rep as sales_rep,
    manager,
    sales_team
from {{ source('silver', 'pharma_sales') }}