# Diccionario de Datos - Lakehouse Farma

Descripción detallada de todas las tablas, columnas y relaciones del sistema.

---

## Índice

1. [Visión general](#visión-general)
2. [Capa Bronze](#capa-bronze)
3. [Capa Silver](#capa-silver)
4. [Capa Gold - Dimensiones](#capa-gold---dimensiones)
5. [Capa Gold - Hechos](#capa-gold---hechos)
6. [Capa Gold - Machine Learning](#capa-gold---machine-learning)
7. [Capa Gold - Marts dbt](#capa-gold---marts-dbt)
8. [Relaciones entre tablas](#relaciones-entre-tablas)

---

## Visión general

El modelo de datos sigue el patrón medallion con tres capas:

| Capa | Propósito | Formato | Ubicación |
|------|-----------|---------|-----------|
| Bronze | Datos crudos, sin transformar | Delta Lake | `/data/bronze/` |
| Silver | Datos limpios y validados | Delta Lake | `/data/silver/` |
| Gold | Modelo dimensional + ML + Marts | Delta Lake | `/data/gold/` |

### Diagrama de flujo de datos

```
pharma-data.csv ──→ pharma_sales_raw ──→ pharma_sales ──┬──→ dim_customer
                                                        ├──→ dim_product
                                                        ├──→ dim_sales_rep
                                                        ├──→ dim_time
                                                        └──→ fact_sales

Kafka (orders) ──→ orders_stream_raw ──→ orders ──────────→ fact_orders
```

---

## Capa Bronze

### pharma_sales_raw

**Ubicación**: `/data/bronze/pharma_sales_raw/`

**Descripción**: Datos crudos del CSV de ventas farmacéuticas. Se preservan tal cual vienen del origen, solo se añaden metadatos de ingesta.

**Origen**: `data/landing/pharma-data.csv` (Kaggle)

**Volumen**: ~250,000 registros

| Columna | Tipo | Descripción | Ejemplo |
|---------|------|-------------|---------|
| distributor | STRING | Nombre del distribuidor | "Salus BG" |
| customer_name | STRING | Nombre del cliente/farmacia | "Pharmadrug Ltd" |
| city | STRING | Ciudad del cliente | "Berlin" |
| country | STRING | País del cliente | "Germany" |
| latitude | DOUBLE | Latitud geográfica | 52.5200 |
| longitude | DOUBLE | Longitud geográfica | 13.4050 |
| channel | STRING | Canal de venta | "Pharmacy" |
| sub_channel | STRING | Subcanal de venta | "Retail" |
| product_name | STRING | Nombre del producto | "Ionclotide" |
| product_class | STRING | Clase terapéutica | "Antiseptics" |
| quantity | INTEGER | Cantidad vendida | 150 |
| price | DOUBLE | Precio unitario | 45.99 |
| sales | DOUBLE | Venta total (quantity * price) | 6898.50 |
| month | STRING | Mes de la venta | "January" |
| year | INTEGER | Año de la venta | 2018 |
| name_of_sales_rep | STRING | Nombre del representante | "Anna Fernandez" |
| manager | STRING | Nombre del manager | "James Carter" |
| sales_team | STRING | Equipo de ventas | "Team Alpha" |
| _ingested_at | TIMESTAMP | Fecha/hora de ingesta | 2026-02-05 00:17:30 |

---

### orders_stream_raw

**Ubicación**: `/data/bronze/orders_stream_raw/`

**Descripción**: Pedidos recibidos en tiempo real desde Kafka. Cada registro es un evento de pedido generado por el simulador.

**Origen**: Topic Kafka `orders`

**Volumen**: Crece continuamente mientras el simulador está activo

| Columna | Tipo | Descripción | Ejemplo |
|---------|------|-------------|---------|
| event_id | STRING | ID único del evento (UUID) | "a1b2c3d4-..." |
| order_id | STRING | ID del pedido | "ord-2026-00001" |
| event_ts | TIMESTAMP | Timestamp del evento | 2026-02-05 10:30:45 |
| customer_id | STRING | ID del cliente | "CUST-0042" |
| product_id | STRING | ID del producto | "PROD-0108" |
| channel | STRING | Canal de venta | "Hospital" |
| quantity | INTEGER | Cantidad pedida | 50 |
| unit_price | DOUBLE | Precio unitario | 32.50 |
| total_amount | DOUBLE | Importe total | 1625.00 |
| currency | STRING | Moneda | "EUR" |
| country | STRING | País | "Spain" |
| _kafka_partition | INTEGER | Partición de Kafka | 0 |
| _kafka_offset | LONG | Offset en Kafka | 12345 |
| _processed_at | TIMESTAMP | Fecha/hora de procesamiento | 2026-02-05 10:30:47 |

---

## Capa Silver

### pharma_sales

**Ubicación**: `/data/silver/pharma_sales/`

**Descripción**: Datos de ventas limpios y validados. Se han aplicado transformaciones de normalización, tipado correcto y reglas de calidad.

**Transformaciones aplicadas**:
- Normalización de nombres de países (ej: "DE" → "Germany")
- Conversión de tipos (quantity a INT, price/sales a DOUBLE)
- Eliminación de duplicados
- Validación: quantity > 0, price >= 0, sales >= 0
- Normalización de canales a valores estándar

**Volumen**: ~250,000 registros (similar a Bronze, menos los rechazados)

| Columna | Tipo | Nullable | Descripción |
|---------|------|----------|-------------|
| distributor | STRING | NO | Distribuidor normalizado |
| customer_name | STRING | NO | Cliente normalizado |
| city | STRING | NO | Ciudad |
| country | STRING | NO | País (nombre completo) |
| latitude | DOUBLE | SI | Latitud (validada en rango) |
| longitude | DOUBLE | SI | Longitud (validada en rango) |
| channel | STRING | NO | Canal: "Pharmacy" o "Hospital" |
| sub_channel | STRING | SI | Subcanal de venta |
| product_name | STRING | NO | Nombre del producto |
| product_class | STRING | NO | Clase terapéutica |
| quantity | INTEGER | NO | Cantidad (validado > 0) |
| price | DOUBLE | NO | Precio unitario (validado >= 0) |
| sales | DOUBLE | NO | Venta total (validado >= 0) |
| month | STRING | NO | Mes de la venta |
| year | INTEGER | NO | Año de la venta |
| name_of_sales_rep | STRING | NO | Representante de ventas |
| manager | STRING | NO | Manager del representante |
| sales_team | STRING | NO | Equipo de ventas |
| _source_file | STRING | NO | Archivo de origen |
| _processed_at | TIMESTAMP | NO | Fecha/hora de procesamiento |

---

### orders

**Ubicación**: `/data/silver/orders/`

**Descripción**: Pedidos de streaming limpios y deduplicados.

**Transformaciones aplicadas**:
- Deduplicación por event_id
- Validación de timestamps
- Normalización de IDs

| Columna | Tipo | Nullable | Descripción |
|---------|------|----------|-------------|
| order_id | STRING | NO | ID único del pedido |
| event_ts | TIMESTAMP | NO | Timestamp del pedido |
| customer_id | STRING | NO | ID del cliente |
| product_id | STRING | NO | ID del producto |
| channel | STRING | NO | Canal de venta |
| quantity | INTEGER | NO | Cantidad pedida |
| unit_price | DOUBLE | NO | Precio unitario |
| total_amount | DOUBLE | NO | Importe total |
| currency | STRING | NO | Moneda (EUR) |
| country | STRING | NO | País |
| _processed_at | TIMESTAMP | NO | Fecha de procesamiento |

---

## Capa Gold - Dimensiones

### dim_customer

**Ubicación**: `/data/gold/dim_customer/`

**Descripción**: Dimensión de clientes/puntos de venta. Cada registro representa un cliente único identificado por la combinación de nombre, ciudad y país.

**Volumen**: ~751 registros

| Columna | Tipo | PK | Descripción |
|---------|------|-----|-------------|
| customer_sk | BIGINT | ✓ | Surrogate key (autogenerada) |
| customer_name | STRING | | Nombre del cliente |
| city | STRING | | Ciudad |
| country | STRING | | País |
| latitude | DOUBLE | | Latitud |
| longitude | DOUBLE | | Longitud |
| channel | STRING | | Canal principal |
| _valid_from | TIMESTAMP | | Inicio de validez (SCD) |
| _valid_to | TIMESTAMP | | Fin de validez (SCD) |
| _is_current | BOOLEAN | | Flag de registro actual |

**Clave natural**: `customer_name + city + country`

---

### dim_product

**Ubicación**: `/data/gold/dim_product/`

**Descripción**: Dimensión de productos farmacéuticos.

**Volumen**: ~241 registros

| Columna | Tipo | PK | Descripción |
|---------|------|-----|-------------|
| product_sk | BIGINT | ✓ | Surrogate key |
| product_name | STRING | | Nombre del producto |
| product_class | STRING | | Clase terapéutica |
| _valid_from | TIMESTAMP | | Inicio de validez |
| _valid_to | TIMESTAMP | | Fin de validez |
| _is_current | BOOLEAN | | Flag de registro actual |

**Clave natural**: `product_name`

**Clases terapéuticas presentes**:
- Antiseptics
- Antipyretics
- Analgesics
- Mood Stabilizers
- Antibiotics
- ... (y otras)

---

### dim_sales_rep

**Ubicación**: `/data/gold/dim_sales_rep/`

**Descripción**: Dimensión de representantes de ventas con su jerarquía (rep → manager → team).

**Volumen**: ~80 registros

| Columna | Tipo | PK | Descripción |
|---------|------|-----|-------------|
| sales_rep_sk | BIGINT | ✓ | Surrogate key |
| sales_rep_name | STRING | | Nombre del representante |
| manager | STRING | | Nombre del manager |
| sales_team | STRING | | Equipo de ventas |
| _valid_from | TIMESTAMP | | Inicio de validez |
| _valid_to | TIMESTAMP | | Fin de validez |
| _is_current | BOOLEAN | | Flag de registro actual |

**Clave natural**: `sales_rep_name`

**Jerarquía**:
```
Sales Team (ej: "Team Alpha")
    └── Manager (ej: "James Carter")
            └── Sales Rep (ej: "Anna Fernandez")
```

---

### dim_time

**Ubicación**: `/data/gold/dim_time/`

**Descripción**: Dimensión de tiempo. Contiene una fila por cada combinación mes-año presente en los datos.

**Volumen**: ~72 registros (6 años × 12 meses)

| Columna | Tipo | PK | Descripción |
|---------|------|-----|-------------|
| time_sk | BIGINT | ✓ | Surrogate key |
| month | STRING | | Nombre del mes ("January", etc.) |
| month_number | INTEGER | | Número del mes (1-12) |
| year | INTEGER | | Año |
| quarter | INTEGER | | Trimestre (1-4) |
| semester | INTEGER | | Semestre (1-2) |
| year_month | STRING | | Concatenación "2018-01" |
| is_q4 | BOOLEAN | | Flag de cuarto trimestre |

**Clave natural**: `month + year`

---

## Capa Gold - Hechos

### fact_sales

**Ubicación**: `/data/gold/fact_sales/`

**Descripción**: Tabla de hechos principal con las transacciones de ventas. Cada registro es una línea de venta.

**Volumen**: ~250,000 registros

**Granularidad**: Una fila por transacción de venta (producto-cliente-fecha)

| Columna | Tipo | FK | Descripción |
|---------|------|-----|-------------|
| sale_sk | BIGINT | | Surrogate key de la venta |
| customer_sk | BIGINT | → dim_customer | FK al cliente |
| product_sk | BIGINT | → dim_product | FK al producto |
| sales_rep_sk | BIGINT | → dim_sales_rep | FK al representante |
| time_sk | BIGINT | → dim_time | FK al tiempo |
| distributor | STRING | | Distribuidor |
| quantity | INTEGER | | Cantidad vendida |
| price | DOUBLE | | Precio unitario |
| sales | DOUBLE | | Importe de la venta |
| _loaded_at | TIMESTAMP | | Fecha de carga |

**Métricas**:
- `quantity`: Unidades vendidas
- `price`: Precio por unidad
- `sales`: Total de la venta (quantity × price)

---

### fact_orders

**Ubicación**: `/data/gold/fact_orders/`

**Descripción**: Tabla de hechos de pedidos recibidos por streaming.

**Volumen**: Variable (crece con el streaming)

**Granularidad**: Una fila por pedido

| Columna | Tipo | FK | Descripción |
|---------|------|-----|-------------|
| order_sk | BIGINT | | Surrogate key del pedido |
| order_id | STRING | | ID natural del pedido |
| customer_id | STRING | | ID del cliente (streaming) |
| product_id | STRING | | ID del producto (streaming) |
| event_ts | TIMESTAMP | | Fecha/hora del pedido |
| channel | STRING | | Canal de venta |
| country | STRING | | País |
| quantity | INTEGER | | Cantidad pedida |
| unit_price | DOUBLE | | Precio unitario |
| total_amount | DOUBLE | | Importe total |
| currency | STRING | | Moneda |
| _loaded_at | TIMESTAMP | | Fecha de carga |

**Nota**: Esta tabla no tiene FKs a las dimensiones porque los IDs del streaming no coinciden con los del batch. En un sistema real habría un proceso de matching.

---

## Capa Gold - Machine Learning

### ml_customer_clusters

**Ubicación**: `/data/gold/ml_customer_clusters/`

**Descripción**: Resultado del modelo de segmentación K-Means. Cada cliente está asignado a uno de los 3 clusters identificados.

**Algoritmo**: K-Means (k=3)

**Métrica de calidad**: Silhouette Score = 0.817

**Volumen**: ~751 registros (uno por cliente)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| customer_id | STRING | ID del cliente (basado en nombre) |
| cluster_id | INTEGER | ID numérico del cluster (0, 1, 2) |
| cluster_label | STRING | Etiqueta descriptiva del cluster |
| total_sales | DOUBLE | Ventas totales del cliente |
| total_qty | BIGINT | Cantidad total comprada |
| transaction_count | BIGINT | Número de transacciones |
| avg_transaction_value | DOUBLE | Ticket medio |
| latitude | DOUBLE | Latitud del cliente |
| longitude | DOUBLE | Longitud del cliente |
| created_at | TIMESTAMP | Fecha de creación del registro |

**Clusters identificados**:

| cluster_id | cluster_label | Descripción |
|------------|---------------|-------------|
| 0 | Premium - Alto Volumen | Clientes top, mayor facturación |
| 1 | Regular - Volumen Medio | Base estable de clientes |
| 2 | Básico - Bajo Volumen | Clientes ocasionales o nuevos |

**Features usadas en el modelo**:
- total_sales (normalizado)
- total_qty (normalizado)
- transaction_count (normalizado)
- avg_transaction_value (normalizado)
- latitude, longitude (normalizados)

---

### ml_demand_forecast

**Ubicación**: `/data/gold/ml_demand_forecast/`

**Descripción**: Predicciones de demanda mensual por producto. Incluye tanto el valor real como el predicho para poder evaluar la precisión.

**Algoritmo**: Gradient Boosted Trees (GBT Regressor)

**Volumen**: ~2,800 registros (productos × meses)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| product_name | STRING | Nombre del producto |
| country | STRING | País |
| year | INTEGER | Año |
| month | INTEGER | Número del mes (1-12) |
| forecast_date | STRING | Fecha del pronóstico "YYYY-MM" |
| actual_sales | DOUBLE | Ventas reales |
| predicted_sales | DOUBLE | Ventas predichas por el modelo |
| absolute_error | DOUBLE | Error absoluto |
| percentage_error | DOUBLE | Error porcentual (MAPE) |
| forecast_accuracy | DOUBLE | Precisión (100 - MAPE) |
| total_quantity | BIGINT | Cantidad total real |
| model_r2 | DOUBLE | R² del modelo |
| model_rmse | DOUBLE | RMSE del modelo |

**Features usadas en el modelo**:
- year, month, quarter
- month_sin, month_cos (encoding cíclico)
- lag_1, lag_2, lag_3 (ventas de meses anteriores)
- rolling_mean_3, rolling_mean_6 (medias móviles)

**Métricas del modelo**:
- R² (coeficiente de determinación)
- RMSE (error cuadrático medio)
- MAPE por producto (error porcentual absoluto medio)

---

## Capa Gold - Marts dbt

### mart_sales_rep_performance

**Ubicación**: Tabla Spark `mart_sales_rep_performance`

**Descripción**: Análisis de rendimiento de representantes de ventas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| sales_rep_sk | BIGINT | SK del representante |
| sales_rep_name | STRING | Nombre del representante |
| manager | STRING | Manager del representante |
| sales_team | STRING | Equipo de ventas |
| total_sales | DOUBLE | Ventas totales |
| total_quantity | BIGINT | Unidades vendidas |
| num_transactions | BIGINT | Número de transacciones |
| num_customers | BIGINT | Clientes únicos atendidos |
| num_products | BIGINT | Productos distintos vendidos |
| avg_sale_value | DOUBLE | Valor promedio por venta |

---

### mart_geographic_analysis

**Ubicación**: Tabla Spark `mart_geographic_analysis`

**Descripción**: Análisis de ventas por geografía.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| country | STRING | País |
| city | STRING | Ciudad |
| total_sales | DOUBLE | Ventas totales |
| total_quantity | BIGINT | Unidades vendidas |
| num_customers | BIGINT | Número de clientes |
| num_transactions | BIGINT | Número de transacciones |
| avg_sale_value | DOUBLE | Ticket medio |

---

### mart_product_performance

**Ubicación**: Tabla Spark `mart_product_performance`

**Descripción**: Análisis de rendimiento de productos.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| product_sk | BIGINT | SK del producto |
| product_name | STRING | Nombre del producto |
| product_class | STRING | Clase terapéutica |
| total_sales | DOUBLE | Ventas totales |
| total_quantity | BIGINT | Unidades vendidas |
| num_transactions | BIGINT | Transacciones |
| num_customers | BIGINT | Clientes que lo compraron |
| avg_price | DOUBLE | Precio promedio |

---

### mart_cluster_analysis

**Ubicación**: Tabla Spark `mart_cluster_analysis`

**Descripción**: Resumen agregado de los clusters de clientes.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| cluster_label | STRING | Etiqueta del cluster |
| num_customers | BIGINT | Clientes en el cluster |
| avg_sales_per_customer | DOUBLE | Ventas promedio por cliente |
| total_cluster_sales | DOUBLE | Ventas totales del cluster |
| avg_transactions | DOUBLE | Transacciones promedio |
| avg_ticket | DOUBLE | Ticket medio |
| avg_quantity | DOUBLE | Cantidad promedio |
| min_sales | DOUBLE | Ventas mínimas |
| max_sales | DOUBLE | Ventas máximas |
| pct_of_total_sales | DOUBLE | % de las ventas totales |
| pct_of_total_customers | DOUBLE | % de los clientes totales |

---

### mart_forecast_analysis

**Ubicación**: Tabla Spark `mart_forecast_analysis`

**Descripción**: Análisis de precisión del modelo de forecast por producto.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| product_name | STRING | Nombre del producto |
| num_predictions | BIGINT | Número de predicciones |
| avg_actual_sales | DOUBLE | Media de ventas reales |
| avg_predicted_sales | DOUBLE | Media de ventas predichas |
| product_mape | DOUBLE | MAPE del producto |
| product_accuracy_pct | DOUBLE | Precisión (%) |
| total_actual_sales | DOUBLE | Total ventas reales |
| total_predicted_sales | DOUBLE | Total ventas predichas |
| accuracy_category | STRING | Categoría de precisión |
| model_r2 | DOUBLE | R² del modelo |
| model_rmse | DOUBLE | RMSE del modelo |

**Categorías de precisión**:
- "Excelente": accuracy >= 90%
- "Bueno": accuracy >= 80%
- "Aceptable": accuracy >= 70%
- "Necesita mejora": accuracy < 70%

---

## Relaciones entre tablas

### Modelo estrella principal

```
                    ┌─────────────────┐
                    │   dim_customer  │
                    │   (customer_sk) │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│   dim_product   │          │          │  dim_sales_rep  │
│  (product_sk)   │──────────┼──────────│ (sales_rep_sk)  │
└─────────────────┘          │          └─────────────────┘
                             │
                    ┌────────┴────────┐
                    │   fact_sales    │
                    │   (sale_sk)     │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │    dim_time     │
                    │    (time_sk)    │
                    └─────────────────┘
```

### Joins típicos

```sql
-- Ventas con todas las dimensiones
SELECT 
    t.year,
    t.month,
    c.customer_name,
    c.country,
    p.product_name,
    p.product_class,
    r.sales_rep_name,
    r.sales_team,
    f.quantity,
    f.sales
FROM fact_sales f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_sales_rep r ON f.sales_rep_sk = r.sales_rep_sk
JOIN dim_time t ON f.time_sk = t.time_sk;
```

---

## Linaje de datos

### pharma-data.csv → fact_sales

```
1. Ingesta (Bronze)
   pharma-data.csv → pharma_sales_raw
   - Lee CSV
   - Añade metadatos de ingesta
   - Escribe en Delta

2. Curación (Silver)
   pharma_sales_raw → pharma_sales
   - Normaliza nombres
   - Valida tipos
   - Aplica reglas de calidad
   - Deduplica

3. Dimensional (Gold)
   pharma_sales → dim_* + fact_sales
   - Extrae dimensiones únicas
   - Genera surrogate keys
   - Construye fact con FKs
```

### Kafka → fact_orders

```
1. Streaming (Bronze)
   Kafka topic "orders" → orders_stream_raw
   - Consume mensajes JSON
   - Escribe en Delta con checkpointing

2. Curación (Silver)
   orders_stream_raw → orders
   - Deduplica por event_id
   - Normaliza campos

3. Dimensional (Gold)
   orders → fact_orders
   - Añade surrogate key
   - Preserva IDs naturales
```

---

*Última actualización: Febrero 2026*
