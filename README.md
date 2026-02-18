# Data Lakehouse para la Distribución Farmacéutica

**Trabajo Fin de Máster** — Máster en Big Data & Data Engineering  
Universidad Complutense de Madrid | Febrero 2026  
Autor: Eric Barba Lopez  
Tutores: Jorge Centeno y Alberto González

## Descripción

Plataforma de datos integral para el sector farmacéutico que implementa una arquitectura lakehouse con patrón medallion (Bronze/Silver/Gold). Procesa ~250.000 registros de ventas históricas y pedidos en tiempo real, incorporando modelos de ML para segmentación de clientes y predicción de demanda, con visualización mediante Power BI Desktop.

## Stack Tecnológico

| Componente | Tecnología | Versión |
|------------|-----------|---------|
| Procesamiento | Apache Spark | 3.5.1 |
| Almacenamiento | Delta Lake | 3.1.0 |
| Streaming | Apache Kafka | 4.1.1 |
| Orquestación | Apache Airflow | 2.10.4 |
| Transformaciones | dbt Core | 1.11.2 |
| ML | Spark MLlib | 3.5.1 |
| Contenedores | Docker Compose | v2 |
| Lenguaje | Python | 3.11 |
| Base de datos | PostgreSQL | 15 |
| Visualización | Power BI Desktop | 2.150.2102.0 |

## Estructura del Proyecto

```
tfm-lakehouse-farma/
├── airflow/
│   └── dags/
│       └── lakehouse_farma_pipeline.py   # DAG de orquestación
├── data/
│   ├── landing/                          # CSV fuente (pharma-data.csv)
│   └── export/                           # CSVs exportados de Gold/Marts
├── dbt_gold/
│   ├── models/
│   │   ├── staging/                      # Vista staging (stg_pharma_sales)
│   │   └── marts/                        # 5 marts analíticos + tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── src/
│   ├── simulator/
│   │   ├── producer.py                   # Generador de eventos Kafka
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── spark/
│       ├── ingest_pharma.py              # Bronze: ingesta batch CSV
│       ├── ingest_shipments.py           # Bronze: ingesta envíos
│       ├── consumer_orders.py            # Bronze: streaming Kafka
│       ├── build_silver_pharma.py        # Silver: curación ventas
│       ├── build_silver_orders.py        # Silver: curación pedidos
│       ├── build_silver_shipments.py     # Silver: curación envíos
│       ├── build_gold.py                 # Gold: modelo dimensional
│       ├── gold_kpis.py                  # Gold: KPIs agregados
│       ├── ml_customer_clustering.py     # ML: K-Means clustering
│       └── ml_demand_forecast.py         # ML: GBT forecasting
├── tests/
│   └── test_pipeline.py                  # 55 tests unitarios pytest
├── docs/
│   ├── arquitectura_datos.svg            # Diagrama de arquitectura
│   ├── diccionario_datos.md              # Descripción de tablas y columnas
│   └── runbook.md                        # Operaciones y troubleshooting
├── docker-compose.yml                    # Definición de servicios (8 contenedores)
├── pytest.ini                            # Configuración pytest
├── requirements-dev.txt                  # Dependencias de test
├── .env                                  # Variables de entorno (AIRFLOW_UID)
├── .gitignore
└── README.md
```

## Despliegue Rápido

### Requisitos

- Docker Desktop (con al menos 8 GB de RAM asignados)
- Git

### Levantar el entorno

```bash
git clone https://github.com/ebarba21/tfm-lakehouse-farma.git
cd tfm-lakehouse-farma
docker compose up -d
```

### Ejecutar el pipeline

1. Acceder a http://localhost:8080 (Airflow)
2. Iniciar sesión: `airflow` / `airflow`
3. Activar el DAG `lakehouse_farma_pipeline`
4. Hacer clic en *Trigger DAG*

El pipeline completo tarda aproximadamente 7 minutos.

### Interfaces disponibles

| Servicio | URL | Credenciales |
|----------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark UI | http://localhost:4040 | — |
| Thrift Server (SQL) | localhost:10000 | — |

## Modelo Dimensional (Gold)

Esquema en estrella con 4 dimensiones y 2 tablas de hechos:

- **dim_customer** (751 clientes) · **dim_product** (241 productos)
- **dim_sales_rep** (13 comerciales) · **dim_time** (49 periodos)
- **fact_sales** (~250K registros) · **fact_orders** (streaming)

## Machine Learning

- **Segmentación de clientes**: K-Means con Spark MLlib → 3 clusters, Silhouette Score = 0.817
- **Predicción de demanda**: Gradient Boosted Trees → precisión media >90% (rango 87-99%)

## Tests

El proyecto cuenta con **76 tests automatizados** en dos capas complementarias.

### Tests unitarios (pytest) — 55 tests

Ubicados en `tests/test_pipeline.py`:

- Funciones puras (`normalize_column_name`, 6 casos)
- Validación del esquema de eventos Kafka (15 campos, tipos)
- Consistencia de rutas entre scripts del pipeline
- Configuración dbt (materialización, conexión Thrift)
- Estructura del proyecto (14 archivos críticos, 5 marts)
- Configuración Kafka con `monkeypatch`
- `PharmaDataCatalog` con CSV mock (fixtures + `tmp_path`)
- Generación de eventos con `mocker.spy`

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

### Tests de calidad de datos (dbt) — 21 tests

Definidos en `_schema.yml` y `_schema_ml.yml`:

- `not_null`, `unique`, `accepted_values`, `relationships`
- Se ejecutan automáticamente como parte del DAG de Airflow

```bash
cd dbt_gold
dbt test --profiles-dir .
```

## Consultas SQL

```bash
# Conectar vía Beeline al Thrift Server
docker exec -it tfm-lakehouse-farma-spark-thrift-1 \
  /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000"
```

```sql
-- Top 10 productos por ventas
SELECT p.product_name, SUM(f.sales) as total_ventas
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.product_name
ORDER BY total_ventas DESC
LIMIT 10;

-- Segmentos de clientes
SELECT * FROM gold.ml_customer_clusters LIMIT 10;

-- Precisión del forecast
SELECT product_name, product_accuracy_pct, accuracy_category
FROM mart_forecast_analysis
ORDER BY product_accuracy_pct DESC;
```

## Visualización (Power BI)

El dashboard `lakehouse_farma_dashboard.pbix` se conecta al Spark Thrift Server vía JDBC y contiene tres páginas:

1. **Resumen Ejecutivo** — KPIs principales, ventas por clase/distribuidor/canal
2. **Marts dbt** — Rankings de productos y comerciales, precisión del forecast
3. **Machine Learning** — Distribución de clusters, ventas por segmento, predicciones vs reales

## Documentación

- [Diagrama de arquitectura](docs/arquitectura_datos.svg)
- [Diccionario de datos](docs/diccionario_datos.md)
- [Runbook de operaciones](docs/runbook.md)

## Apagar el entorno

```bash
docker compose down       # Mantiene datos
docker compose down -v    # Elimina datos y volúmenes
```
