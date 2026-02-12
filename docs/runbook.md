# Runbook de Operaciones - Lakehouse Farma

Guía práctica para operar, mantener y solucionar problemas del sistema.

---

## Índice

1. [Comandos del día a día](#comandos-del-día-a-día)
2. [Gestión de servicios](#gestión-de-servicios)
3. [Monitorización](#monitorización)
4. [Troubleshooting](#troubleshooting)
5. [Mantenimiento](#mantenimiento)
6. [Recuperación ante fallos](#recuperación-ante-fallos)

---

## Comandos del día a día

### Levantar el entorno

```bash
cd C:\Users\lopez\tfm-lakehouse-farma
docker compose up -d
```

Esperar ~2 minutos a que Airflow esté listo. Se puede verificar con:

```bash
docker compose ps
```

Todos los servicios deben estar en estado "Up" o "healthy".

### Apagar el entorno

```bash
docker compose down
```

Esto para los contenedores pero mantiene los datos (volúmenes). Si quieres borrar todo y empezar de cero:

```bash
docker compose down -v
```

**Cuidado**: El `-v` borra todos los datos de las tablas Delta.

### Ver logs de un servicio

```bash
# Logs del scheduler de Airflow
docker logs tfm-lakehouse-farma-airflow-scheduler-1 --tail 100

# Logs de Spark
docker logs tfm-lakehouse-farma-spark-1 --tail 100

# Logs de Kafka
docker logs tfm-lakehouse-farma-kafka-1 --tail 100

# Logs del simulador de pedidos
docker logs tfm-lakehouse-farma-simulator-1 --tail 100

# Seguir logs en tiempo real (útil para debugging)
docker logs tfm-lakehouse-farma-spark-1 -f
```

### Ejecutar el pipeline manualmente

Opción 1 - Desde la UI de Airflow:
1. Ir a http://localhost:8080
2. Login: airflow / airflow
3. Buscar `lakehouse_farma_pipeline`
4. Click en "Trigger DAG"

Opción 2 - Desde línea de comandos:

```bash
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow dags trigger lakehouse_farma_pipeline
```

### Ejecutar una tarea específica del DAG

Si solo quieres re-ejecutar una parte del pipeline:

```bash
# Ejemplo: solo el modelo dimensional
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow tasks run lakehouse_farma_pipeline gold_layer.build_dimensional_model manual__2026-02-05T00:00:00+00:00
```

### Consultar tablas con Beeline

```bash
# Abrir consola SQL interactiva
docker exec -it tfm-lakehouse-farma-spark-thrift-1 /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000"

# Ejecutar una query directamente
docker exec tfm-lakehouse-farma-spark-thrift-1 /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "SELECT COUNT(*) FROM fact_sales;"
```

### Ejecutar dbt manualmente

```bash
cd C:\Users\lopez\tfm-lakehouse-farma\dbt_gold

# Ejecutar todos los modelos
dbt run --profiles-dir .

# Ejecutar tests
dbt test --profiles-dir .

# Ejecutar un modelo específico
dbt run --select mart_cluster_analysis --profiles-dir .

# Ver la documentación generada
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

---

## Gestión de servicios

### Contenedores del sistema

| Contenedor | Servicio | Puerto | Función |
|------------|----------|--------|---------|
| tfm-lakehouse-farma-airflow-webserver-1 | Airflow UI | 8080 | Interfaz web de Airflow |
| tfm-lakehouse-farma-airflow-scheduler-1 | Airflow Scheduler | - | Ejecuta los DAGs |
| tfm-lakehouse-farma-spark-1 | Spark Master | 4040 | Procesamiento de datos |
| tfm-lakehouse-farma-spark-thrift-1 | Spark Thrift Server | 10000 | Conexiones JDBC/SQL |
| tfm-lakehouse-farma-kafka-1 | Kafka | 9092 | Mensajería streaming |
| tfm-lakehouse-farma-postgres-1 | PostgreSQL | 5432 | Metadatos de Airflow |
| tfm-lakehouse-farma-simulator-1 | Simulador | - | Genera pedidos a Kafka |

### Reiniciar un servicio específico

```bash
# Reiniciar solo Spark
docker restart tfm-lakehouse-farma-spark-1

# Reiniciar el simulador (útil si se desconecta de Kafka)
docker restart tfm-lakehouse-farma-simulator-1

# Reiniciar Airflow (ambos contenedores)
docker restart tfm-lakehouse-farma-airflow-scheduler-1
docker restart tfm-lakehouse-farma-airflow-webserver-1
```

### Ver el estado de los DAGs

```bash
# Listar todos los DAGs
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow dags list

# Ver errores de importación (si un DAG no aparece)
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow dags list-import-errors

# Pausar/despausar un DAG
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow dags pause lakehouse_farma_pipeline
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow dags unpause lakehouse_farma_pipeline
```

### Verificar conectividad de Kafka

```bash
# Ver topics existentes
docker exec tfm-lakehouse-farma-kafka-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Ver mensajes del topic orders (últimos 10)
docker exec tfm-lakehouse-farma-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 10
```

---

## Monitorización

### URLs de monitorización

| Servicio | URL | Qué ver |
|----------|-----|---------|
| Airflow | http://localhost:8080 | Estado de DAGs, logs de tareas |
| Spark Master | http://localhost:4040 | Jobs activos, stages, storage |
| Spark Thrift | http://localhost:4041 | Queries SQL en ejecución |

### Verificar que todo funciona

Script rápido de health check:

```bash
# 1. Verificar contenedores
docker compose ps

# 2. Verificar que Airflow responde
curl -s http://localhost:8080/health | findstr healthy

# 3. Verificar que Spark Thrift responde
docker exec tfm-lakehouse-farma-spark-thrift-1 /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "SELECT 1;"

# 4. Verificar que hay datos en las tablas principales
docker exec tfm-lakehouse-farma-spark-thrift-1 /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "SELECT COUNT(*) as registros FROM fact_sales;"
```

### Métricas clave a revisar

**Volumen de datos esperado:**

| Tabla | Registros esperados |
|-------|---------------------|
| bronze.pharma_sales_raw | ~250,000 |
| silver.pharma_sales | ~250,000 |
| gold.fact_sales | ~250,000 |
| gold.dim_customer | ~750 |
| gold.dim_product | ~240 |
| gold.ml_customer_clusters | ~750 |
| gold.ml_demand_forecast | ~2,800 |

```sql
-- Query para verificar volúmenes
SELECT 'fact_sales' as tabla, COUNT(*) as registros FROM fact_sales
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'ml_customer_clusters', COUNT(*) FROM ml_customer_clusters
UNION ALL
SELECT 'ml_demand_forecast', COUNT(*) FROM ml_demand_forecast;
```

---

## Troubleshooting

### Problema: El DAG no aparece en Airflow

**Síntoma**: El DAG `lakehouse_farma_pipeline` no se ve en la UI.

**Diagnóstico**:
```bash
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow dags list-import-errors
```

**Causas comunes**:
1. Error de sintaxis en el archivo Python
2. Import de módulo que no existe
3. Error de indentación

**Solución**: Revisar el error mostrado y corregir el archivo `airflow/dags/lakehouse_farma_pipeline.py`

---

### Problema: Tarea falla con "Table not found"

**Síntoma**: dbt o las queries fallan porque no encuentran una tabla.

**Diagnóstico**:
```bash
docker exec tfm-lakehouse-farma-spark-thrift-1 /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES;"
```

**Causa**: Las tablas Delta existen en disco pero no están registradas en el metastore de Spark.

**Solución**: Registrar las tablas manualmente:

```bash
docker exec tfm-lakehouse-farma-spark-thrift-1 /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "
CREATE TABLE IF NOT EXISTS fact_sales USING DELTA LOCATION '/data/gold/fact_sales';
CREATE TABLE IF NOT EXISTS dim_customer USING DELTA LOCATION '/data/gold/dim_customer';
CREATE TABLE IF NOT EXISTS dim_product USING DELTA LOCATION '/data/gold/dim_product';
CREATE TABLE IF NOT EXISTS dim_sales_rep USING DELTA LOCATION '/data/gold/dim_sales_rep';
CREATE TABLE IF NOT EXISTS dim_time USING DELTA LOCATION '/data/gold/dim_time';
CREATE TABLE IF NOT EXISTS ml_customer_clusters USING DELTA LOCATION '/data/gold/ml_customer_clusters';
CREATE TABLE IF NOT EXISTS ml_demand_forecast USING DELTA LOCATION '/data/gold/ml_demand_forecast';
"
```

---

### Problema: Spark se queda sin memoria

**Síntoma**: Error "Java heap space" o "OutOfMemoryError"

**Diagnóstico**: Ver logs de Spark:
```bash
docker logs tfm-lakehouse-farma-spark-1 --tail 200 | findstr -i "memory\|heap\|oom"
```

**Solución**:

1. Aumentar memoria de Docker Desktop (Settings > Resources > Memory > 12GB o más)

2. Reiniciar los contenedores:
```bash
docker compose down
docker compose up -d
```

3. Si persiste, reducir paralelismo en los scripts Spark:
```python
spark.conf.set("spark.sql.shuffle.partitions", "8")
```

---

### Problema: El simulador no envía mensajes a Kafka

**Síntoma**: No llegan pedidos nuevos al topic `orders`.

**Diagnóstico**:
```bash
# Ver logs del simulador
docker logs tfm-lakehouse-farma-simulator-1 --tail 50

# Verificar que Kafka está corriendo
docker exec tfm-lakehouse-farma-kafka-1 /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

**Causas comunes**:
1. Kafka no había arrancado cuando el simulador intentó conectar
2. El topic `orders` no existe

**Solución**:
```bash
# Reiniciar el simulador
docker restart tfm-lakehouse-farma-simulator-1

# Si el topic no existe, crearlo manualmente
docker exec tfm-lakehouse-farma-kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

### Problema: dbt test falla con "accepted_values"

**Síntoma**: Test de accepted_values falla porque los valores no coinciden.

**Diagnóstico**:
```sql
-- Ver qué valores hay realmente
SELECT DISTINCT cluster_label FROM ml_customer_clusters;
```

**Causa**: Los valores en la tabla no coinciden exactamente con los definidos en `_schema_ml.yml` (puede ser por encoding, espacios, etc.)

**Solución**: Actualizar el archivo `dbt_gold/models/marts/_schema_ml.yml` con los valores correctos.

---

### Problema: No puedo conectar DBeaver al Thrift Server

**Síntoma**: DBeaver da timeout o "Connection refused".

**Diagnóstico**:
```bash
# Verificar que el puerto está abierto
docker port tfm-lakehouse-farma-spark-thrift-1

# Verificar que el Thrift Server está corriendo
docker logs tfm-lakehouse-farma-spark-thrift-1 --tail 50
```

**Solución**:

1. Verificar configuración de DBeaver:
   - Host: `localhost`
   - Puerto: `10000`
   - Database: `default`
   - Driver: Apache Hive

2. Si el Thrift Server no arrancó bien:
```bash
docker restart tfm-lakehouse-farma-spark-thrift-1
```

3. Esperar ~30 segundos y reintentar.

---

### Problema: Pipeline de Airflow se queda "stuck" en running

**Síntoma**: Una tarea lleva mucho tiempo en estado "running" sin avanzar.

**Diagnóstico**:
```bash
# Ver qué tareas están corriendo
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow tasks states-for-dag-run lakehouse_farma_pipeline <run_id>
```

**Solución**:

1. Marcar la tarea como failed desde la UI de Airflow y re-ejecutar

2. O desde línea de comandos:
```bash
# Limpiar la tarea para que se pueda re-ejecutar
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow tasks clear lakehouse_farma_pipeline -t <task_id> -s <start_date> -e <end_date>
```

---

## Mantenimiento

### Limpieza de logs antiguos

Los logs de Airflow pueden crecer bastante. Para limpiarlos:

```bash
# Borrar logs de más de 7 días
find airflow/logs -type f -mtime +7 -delete
```

### Vacuuming de tablas Delta

Delta Lake acumula archivos antiguos. Para limpiarlos:

```python
# Ejecutar en spark-shell o en un script
from delta.tables import DeltaTable

# Ejemplo para fact_sales
dt = DeltaTable.forPath(spark, "/data/gold/fact_sales")
dt.vacuum(168)  # Borra archivos de más de 7 días (168 horas)
```

**Nota**: Por defecto Delta no permite vacuum de menos de 7 días. No cambiar esto en producción.

### Backup de datos

Para hacer backup de las tablas Delta:

```bash
# Copiar toda la carpeta data
xcopy /E /I C:\Users\lopez\tfm-lakehouse-farma\data C:\backup\lakehouse-farma-data
```

### Actualizar el código de los scripts

Si modificas algún script de `src/spark/`:

1. No hace falta rebuild de contenedores (el directorio está montado)
2. Solo hay que re-ejecutar el pipeline o la tarea específica

Si modificas el `docker-compose.yml` o el `Dockerfile` del simulador:

```bash
docker compose down
docker compose up -d --build
```

---

## Recuperación ante fallos

### Escenario: Se corrompió una tabla Delta

**Síntomas**: Errores de "corrupted" o "cannot read parquet file"

**Solución**:

1. Identificar la tabla afectada

2. Restaurar desde el historial de Delta (si existe):
```python
# Ver historial
spark.sql("DESCRIBE HISTORY fact_sales").show()

# Restaurar a una versión anterior
spark.sql("RESTORE TABLE fact_sales TO VERSION AS OF 5")
```

3. Si no funciona, borrar la tabla y re-ejecutar el pipeline:
```bash
# Borrar la carpeta de la tabla
rmdir /S /Q C:\Users\lopez\tfm-lakehouse-farma\data\gold\fact_sales

# Re-ejecutar el pipeline desde gold_layer
```

### Escenario: Docker no arranca

**Síntomas**: "Cannot connect to Docker daemon"

**Solución**:

1. Verificar que Docker Desktop está corriendo
2. Reiniciar Docker Desktop
3. Si sigue fallando, reiniciar el ordenador

### Escenario: Hay que empezar de cero

Para resetear completamente el proyecto:

```bash
# Parar todo y borrar volúmenes
docker compose down -v

# Borrar datos locales
rmdir /S /Q data\bronze
rmdir /S /Q data\silver
rmdir /S /Q data\gold
rmdir /S /Q data\checkpoints

# Recrear estructura
mkdir data\bronze data\silver data\gold data\checkpoints

# Volver a levantar
docker compose up -d

# Ejecutar pipeline
# (desde Airflow UI o CLI)
```

---

## Comandos de referencia rápida

```bash
# Levantar entorno
docker compose up -d

# Apagar entorno
docker compose down

# Ver estado de contenedores
docker compose ps

# Ver logs de un servicio
docker logs <nombre-contenedor> --tail 100

# Ejecutar query SQL
docker exec tfm-lakehouse-farma-spark-thrift-1 /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -e "<QUERY>"

# Trigger del DAG
docker exec tfm-lakehouse-farma-airflow-scheduler-1 airflow dags trigger lakehouse_farma_pipeline

# dbt run y test
cd dbt_gold && dbt run --profiles-dir . && dbt test --profiles-dir .

# Reiniciar servicio problemático
docker restart <nombre-contenedor>
```

---

*Última actualización: Febrero 2026*
