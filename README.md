# Data Lakehouse para la Distribucion Farmaceutica

**Trabajo Fin de Master** — Master en Big Data & Data Engineering  
Universidad Complutense de Madrid | Febrero 2026  
Autor: Eric Barba Lopez  
Tutores: Jorge Centeno y Alberto Gonzalez

## Descripcion

Plataforma de datos integral para el sector farmaceutico que implementa una arquitectura lakehouse con patron medallion (Bronze/Silver/Gold). Procesa ~250.000 registros de ventas historicas y pedidos en tiempo real, incorporando modelos de ML para segmentacion de clientes y prediccion de demanda.

## Stack Tecnologico

| Componente | Tecnologia | Version |
|------------|-----------|---------|
| Procesamiento | Apache Spark | 3.5.1 |
| Almacenamiento | Delta Lake | 3.1.0 |
| Streaming | Apache Kafka | 4.1.1 |
| Orquestacion | Apache Airflow | 2.10.4 |
| Transformaciones | dbt Core | 1.11.2 |
| ML | Spark MLlib | 3.5.1 |
| Contenedores | Docker Compose | v2 |

## Estructura del Proyecto
```
tfm-lakehouse-farma/
├── airflow/dags/              # DAG de orquestacion
├── data/landing/              # CSV fuente (pharma-data.csv)
├── dbt_gold/models/           # Modelos dbt (5 marts + tests)
├── src/spark/                 # Scripts de procesamiento
│   ├── bronze/                # Ingesta batch y streaming
│   ├── silver/                # Curacion y validacion
│   ├── gold/                  # Modelo dimensional
│   └── ml/                    # Clustering y forecasting
├── src/simulator/             # Generador de eventos Kafka
├── docs/                      # Documentacion tecnica
└── docker-compose.yml         # Definicion de servicios
```

## Despliegue Rapido
```bash
# Levantar todos los servicios
docker compose up -d

# Acceder a Airflow (admin/admin)
# http://localhost:8080

# Activar el DAG lakehouse_farma_pipeline y ejecutar
```

## Modelo Dimensional (Gold)

Esquema en estrella con 4 dimensiones y 2 tablas de hechos:
- **dim_customer** (751 clientes) | **dim_product** (241 productos)
- **dim_sales_rep** (~80 comerciales) | **dim_time** (72 periodos)
- **fact_sales** (~250K registros) | **fact_orders** (streaming)

## Machine Learning

- **Segmentacion de clientes**: K-Means, 3 clusters, Silhouette Score = 0.817
- **Prediccion de demanda**: Gradient Boosted Trees, precision media >90%

## Calidad de Datos

21 tests automatizados con dbt (not_null, unique, accepted_values, relationships).

## Consultas SQL
```bash
# Conectar via Beeline al Thrift Server
docker exec -it tfm-lakehouse-farma-spark-thrift-1 \
  /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000"
```

## Documentacion

- [Runbook de operaciones](docs/runbook.md)
- [Diccionario de datos](docs/diccionario_datos.md)
- [Diagrama de arquitectura](docs/arquitectura_datos.svg)
