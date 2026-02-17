"""
DAG que orquesta el pipeline completo del lakehouse farmacéutico.
Secuencia: Bronze -> Silver -> Gold -> ML -> dbt
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

SPARK_CONTAINER = "tfm-lakehouse-farma-spark-1"
SPARK_THRIFT_CONTAINER = "tfm-lakehouse-farma-spark-thrift-1"

SPARK_SUBMIT_BASE = f"""
docker exec {SPARK_CONTAINER} bash -lc '/opt/spark/bin/spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /app/src/spark/{{script}}'
"""

with DAG(
    dag_id='lakehouse_farma_pipeline',
    default_args=default_args,
    description='Pipeline completo del lakehouse farmacéutico',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['lakehouse', 'farma', 'tfm', 'delta-lake'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # BRONZE
    with TaskGroup(group_id='bronze_layer') as bronze_group:
        ingest_pharma = BashOperator(
            task_id='ingest_pharma_sales',
            bash_command=SPARK_SUBMIT_BASE.format(script='ingest_pharma.py'),
        )

    # SILVER
    with TaskGroup(group_id='silver_layer') as silver_group:
        curate_pharma = BashOperator(
            task_id='curate_pharma_sales',
            bash_command=SPARK_SUBMIT_BASE.format(script='build_silver_pharma.py'),
        )
        curate_orders = BashOperator(
            task_id='curate_orders',
            bash_command=SPARK_SUBMIT_BASE.format(script='build_silver_orders.py'),
        )

    # GOLD - modelo dimensional
    with TaskGroup(group_id='gold_layer') as gold_group:
        build_gold = BashOperator(
            task_id='build_dimensional_model',
            bash_command=SPARK_SUBMIT_BASE.format(script='build_gold.py'),
        )

    # ML - clustering y forecast en paralelo
    with TaskGroup(group_id='ml_layer') as ml_group:

        customer_clustering = BashOperator(
            task_id='customer_clustering',
            bash_command=f"""
            docker exec --user root {SPARK_CONTAINER} bash -lc 'pip install --quiet numpy && \
            /opt/spark/bin/spark-submit \
                --packages io.delta:delta-spark_2.12:3.1.0 \
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
                /app/src/spark/ml_customer_clustering.py'
            """,
        )

        demand_forecast = BashOperator(
            task_id='demand_forecast',
            bash_command=f"""
            docker exec --user root {SPARK_CONTAINER} bash -lc 'pip install --quiet numpy && \
            /opt/spark/bin/spark-submit \
                --packages io.delta:delta-spark_2.12:3.1.0 \
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
                /app/src/spark/ml_demand_forecast.py'
            """,
        )

        # Los dos modelos pueden correr en paralelo
        [customer_clustering, demand_forecast]

    # DBT - registro de tablas, transformaciones y tests
    with TaskGroup(group_id='dbt_layer') as dbt_group:

        cleanup_warehouse = BashOperator(
            task_id='cleanup_spark_warehouse',
            bash_command="""
            docker run --rm -v tfm-lakehouse-farma_spark-warehouse:/data alpine \
                sh -c "rm -rf /data/mart_* /data/stg_*"
            """,
        )

        register_tables = BashOperator(
            task_id='register_delta_tables',
            bash_command=f"""
            docker exec {SPARK_THRIFT_CONTAINER} /opt/spark/bin/beeline \
                -u "jdbc:hive2://localhost:10000" \
                -e "
CREATE TABLE IF NOT EXISTS silver_pharma_sales USING DELTA LOCATION '/data/silver/pharma_sales';
CREATE TABLE IF NOT EXISTS silver_orders USING DELTA LOCATION '/data/silver/orders';
CREATE TABLE IF NOT EXISTS gold_dim_time USING DELTA LOCATION '/data/gold/dim_time';
CREATE TABLE IF NOT EXISTS gold_dim_product USING DELTA LOCATION '/data/gold/dim_product';
CREATE TABLE IF NOT EXISTS gold_dim_customer USING DELTA LOCATION '/data/gold/dim_customer';
CREATE TABLE IF NOT EXISTS gold_dim_sales_rep USING DELTA LOCATION '/data/gold/dim_sales_rep';
CREATE TABLE IF NOT EXISTS gold_fact_sales USING DELTA LOCATION '/data/gold/fact_sales';
CREATE TABLE IF NOT EXISTS gold_fact_orders USING DELTA LOCATION '/data/gold/fact_orders';
CREATE TABLE IF NOT EXISTS gold_ml_customer_clusters USING DELTA LOCATION '/data/gold/ml_customer_clusters';
CREATE TABLE IF NOT EXISTS gold_ml_demand_forecast USING DELTA LOCATION '/data/gold/ml_demand_forecast';
"
            """,
        )

        dbt_run = BashOperator(
            task_id='dbt_run',
            bash_command=f"""
            docker exec {SPARK_THRIFT_CONTAINER} bash -c '\
                pip install --quiet dbt-core "dbt-spark[PyHive]" && \
                cd /app/dbt_gold && \
                dbt run --profiles-dir .'
            """,
        )

        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command=f"""
            docker exec {SPARK_THRIFT_CONTAINER} bash -c '\
                cd /app/dbt_gold && \
                dbt test --profiles-dir .'
            """,
        )

        cleanup_warehouse >> register_tables >> dbt_run >> dbt_test

    # Dependencias
    start >> bronze_group >> silver_group >> gold_group >> ml_group >> dbt_group >> end