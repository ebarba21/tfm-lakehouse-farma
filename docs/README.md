# Lakehouse Farma

Plataforma de datos para el sector farmacéutico - TFM Máster Data Engineering UCM

## Qué es esto

Este es mi Trabajo Fin de Máster. La idea era montar una arquitectura lakehouse completa para una empresa farmacéutica ficticia, con datos de ventas, pedidos en streaming, y un par de modelos de ML.

Al principio el plan era hacerlo en Azure con Databricks, pero me quedé sin créditos a mitad del proyecto así que tuve que pivotar a una solución 100% local con Docker. Al final creo que fue mejor decisión porque cualquiera puede clonar el repo y levantarlo sin necesidad de cuenta en ningún cloud.

## Stack técnico

- **Spark 3.5.1** - Para todo el procesamiento de datos
- **Delta Lake** - Formato de almacenamiento (el que usa Databricks por debajo)
- **Kafka** - Para el streaming de pedidos
- **Airflow** - Orquestación del pipeline
- **dbt** - Transformaciones y tests en la capa analítica
- **Docker Compose** - Para levantar todo junto

Todo corre en contenedores. Hace falta un ordenador con al menos 8GB de RAM porque si no Spark se queja bastante.

## Arquitectura

El proyecto sigue el patrón medallion que vimos en clase:

```
Landing (CSVs) ──→ Bronze (raw) ──→ Silver (limpio) ──→ Gold (dimensional + ML)
                      ↑
                    Kafka
                  (pedidos)
```

**Bronze**: Los datos tal cual llegan, sin tocar nada. Solo se convierten a Delta.

**Silver**: Aquí se limpian los datos, se normalizan nombres de columnas, se validan tipos, etc. También hay una tabla de pedidos que viene del streaming de Kafka.

**Gold**: Modelo dimensional tipo estrella con sus facts y dims. Además están las tablas de ML (clustering de clientes y forecasting de demanda).

## Los datos

Usé un dataset de Kaggle de ventas farmacéuticas. Son casi 250.000 registros de ventas con info de productos, clientes, representantes comerciales, países, etc.

Para el streaming monté un simulador en Python que genera pedidos falsos y los manda a Kafka. Nada sofisticado pero cumple su función para la demo.

### Números

- 249.704 registros de ventas
- 751 clientes
- 241 productos
- 8 países (Alemania, Francia, Italia, Polonia...)

## Machine Learning

Implementé dos modelos:

**1. Segmentación de clientes (K-Means)**

Agrupa a los clientes en 3 segmentos según su comportamiento de compra (ventas totales, frecuencia, ticket medio...). El silhouette score salió 0.817 que está bastante bien.

Los segmentos que salen son:
- Premium - Alto Volumen
- Regular - Volumen Medio  
- Básico - Bajo Volumen

**2. Forecast de demanda (Gradient Boosted Trees)**

Predice ventas mensuales por producto. Usé features temporales (mes, año, trimestre) más lags y medias móviles. La precisión varía por producto pero la mayoría están entre 87-99%.

Tuve bastantes problemas con este modelo al principio. El GBT de Spark no acepta bien variables categóricas con muchos valores (tenía 241 productos distintos) así que al final tuve que usar solo features numéricos.

## Cómo ejecutarlo

### Requisitos

- Docker Desktop
- Git
- Paciencia (la primera vez tarda un rato en descargar las imágenes)

### Pasos

```bash
# Clonar
git clone https://github.com/tu-usuario/tfm-lakehouse-farma.git
cd tfm-lakehouse-farma

# Levantar todo
docker compose up -d

# Esperar un par de minutos a que arranque Airflow
```

### Ejecutar el pipeline

1. Ir a http://localhost:8080 (Airflow)
2. Login: airflow / airflow
3. Buscar el DAG `lakehouse_farma_pipeline`
4. Darle al play

Tarda unos 7 minutos en ejecutar todo. Se puede ir viendo el progreso en la vista Graph.

### Acceder a los datos

Para consultar las tablas uso DBeaver conectado al Thrift Server de Spark:

```
Host: localhost
Puerto: 10000
Base de datos: default
```

## Estructura del proyecto

```
tfm-lakehouse-farma/
├── airflow/
│   └── dags/
│       └── lakehouse_farma_pipeline.py
├── data/
│   ├── landing/          # CSV original
│   ├── bronze/           # Datos raw en Delta
│   ├── silver/           # Datos limpios
│   └── gold/             # Modelo dimensional + ML
├── dbt_gold/
│   └── models/
│       ├── staging/
│       └── marts/        # Los 5 marts analíticos
├── src/
│   ├── simulator/        # Generador de eventos Kafka
│   └── spark/            # Scripts de procesamiento
└── docker-compose.yml
```

## dbt

Hay 5 marts analíticos:

- `mart_sales_rep_performance` - Rendimiento de comerciales
- `mart_geographic_analysis` - Análisis por país/ciudad
- `mart_product_performance` - Productos más vendidos
- `mart_cluster_analysis` - Resumen de los clusters de ML
- `mart_forecast_analysis` - Precisión del modelo de forecast

Los tests de dbt (21 en total) comprueban cosas básicas: que no haya nulos donde no debe, que las claves sean únicas, que los valores estén dentro de lo esperado...

```bash
# Para ejecutar los tests
cd dbt_gold
dbt test --profiles-dir .
```

## Consultas útiles

```sql
-- Ver los clusters de clientes
SELECT * FROM mart_cluster_analysis;

-- Top 10 productos
SELECT p.product_name, SUM(f.sales) as ventas
FROM fact_sales f
JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY p.product_name
ORDER BY ventas DESC
LIMIT 10;

-- Precisión del forecast por producto
SELECT product_name, product_accuracy_pct, accuracy_category
FROM mart_forecast_analysis
WHERE accuracy_category = 'Excelente';
```

## Problemas conocidos

- A veces Kafka tarda en arrancar y el simulador falla. Reiniciar el contenedor del simulador suele arreglarlo.
- Si Spark se queda sin memoria, hay que aumentar los recursos de Docker.
- El Thrift Server a veces pierde las tablas registradas al reiniciar. Hay que volver a ejecutar los CREATE TABLE.

## Qué me hubiera gustado añadir

Si hubiera tenido más tiempo:

- Dashboard con Metabase o Superset
- Más tests unitarios con pytest
- CI/CD con GitHub Actions
- Despliegue en cloud (el código está preparado para que sea fácil migrarlo)

## Autor

Eric López - Máster en Data Engineering, UCM (2025-2026)

---

*Proyecto académico - TFM*