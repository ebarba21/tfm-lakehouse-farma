"""
Tests unitarios del pipeline Lakehouse Farma.

Validan funciones puras, consistencia de configuracion,
esquemas, generacion de eventos y estructura del proyecto.

Ejecucion:
    pip install pytest pytest-mock pyyaml pyspark faker
    pytest tests/ -v
"""

import csv
import os
import sys
import yaml
import pytest
from pytest_mock import MockerFixture

# Ruta raiz del proyecto
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Importar modulos del pipeline
sys.path.insert(0, os.path.join(ROOT_DIR, "src", "spark"))
sys.path.insert(0, os.path.join(ROOT_DIR, "src", "simulator"))

from ingest_pharma import normalize_column_name
from consumer_orders import ORDER_EVENT_SCHEMA
from ingest_pharma import LANDING_PATH, BRONZE_PATH as INGEST_BRONZE
from build_silver_pharma import BRONZE_PATH as SILVER_INPUT, SILVER_PATH
import consumer_orders
import producer as producer_module


# ---------------------------------------------------------------------------
# 1. Test de funciones puras (normalize_column_name)
# ---------------------------------------------------------------------------

class TestNormalizeColumnName:
    """Pruebas de la funcion de normalizacion a snake_case."""

    def test_espacios_a_guion_bajo(self):
        assert normalize_column_name("Product Name") == "product_name"

    def test_guiones_a_guion_bajo(self):
        assert normalize_column_name("Sub-Channel") == "sub_channel"

    def test_barras_a_guion_bajo(self):
        assert normalize_column_name("City/Town") == "city_town"

    def test_mayusculas_a_minusculas(self):
        assert normalize_column_name("QUANTITY") == "quantity"

    def test_nombre_ya_normalizado(self):
        assert normalize_column_name("customer_name") == "customer_name"

    def test_combinacion_de_caracteres(self):
        assert normalize_column_name("Sales Rep Name/ID") == "sales_rep_name_id"


# ---------------------------------------------------------------------------
# 2. Test del esquema de eventos de Kafka
# ---------------------------------------------------------------------------

class TestOrderEventSchema:
    """Valida el esquema de eventos de pedidos (consumer_orders.py)."""

    EXPECTED_FIELDS = [
        "event_id", "order_id", "event_ts",
        "customer_name", "city", "country", "channel", "sub_channel",
        "product_name", "product_class", "qty", "unit_price",
        "currency", "latitude", "longitude",
    ]

    def test_schema_tiene_todos_los_campos(self):
        field_names = [f.name for f in ORDER_EVENT_SCHEMA.fields]
        for expected in self.EXPECTED_FIELDS:
            assert expected in field_names, f"Falta el campo '{expected}' en ORDER_EVENT_SCHEMA"

    def test_schema_no_tiene_campos_extra(self):
        field_names = [f.name for f in ORDER_EVENT_SCHEMA.fields]
        for actual in field_names:
            assert actual in self.EXPECTED_FIELDS, f"Campo inesperado: '{actual}'"

    def test_campos_numericos_son_tipo_correcto(self):
        type_map = {f.name: f.dataType.simpleString() for f in ORDER_EVENT_SCHEMA.fields}
        assert type_map["qty"] == "int", "qty debe ser IntegerType"
        assert type_map["unit_price"] == "double", "unit_price debe ser DoubleType"
        assert type_map["latitude"] == "double", "latitude debe ser DoubleType"
        assert type_map["longitude"] == "double", "longitude debe ser DoubleType"

    def test_total_campos(self):
        assert len(ORDER_EVENT_SCHEMA.fields) == 15


# ---------------------------------------------------------------------------
# 3. Test de consistencia de rutas entre scripts
# ---------------------------------------------------------------------------

class TestPathConsistency:
    """Verifica que las rutas entre scripts son consistentes."""

    def test_bronze_output_coincide_con_silver_input(self):
        assert INGEST_BRONZE == SILVER_INPUT, (
            f"ingest_pharma escribe a '{INGEST_BRONZE}' pero "
            f"build_silver_pharma lee de '{SILVER_INPUT}'"
        )

    def test_landing_path_apunta_a_csv(self):
        assert LANDING_PATH.endswith(".csv"), "LANDING_PATH debe apuntar a un .csv"

    def test_rutas_usan_formato_absoluto(self):
        assert LANDING_PATH.startswith("/"), "LANDING_PATH debe ser ruta absoluta"
        assert INGEST_BRONZE.startswith("/"), "BRONZE_PATH debe ser ruta absoluta"
        assert SILVER_PATH.startswith("/"), "SILVER_PATH debe ser ruta absoluta"


# ---------------------------------------------------------------------------
# 4. Test de la configuracion dbt
# ---------------------------------------------------------------------------

class TestDbtConfig:
    """Valida la estructura del proyecto dbt."""

    @pytest.fixture
    def dbt_project(self):
        path = os.path.join(ROOT_DIR, "dbt_gold", "dbt_project.yml")
        with open(path, "r") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def dbt_profile(self):
        path = os.path.join(ROOT_DIR, "dbt_gold", "profiles.yml")
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def test_nombre_proyecto(self, dbt_project):
        assert dbt_project["name"] == "dbt_gold"

    def test_materializacion_staging_es_view(self, dbt_project):
        mat = dbt_project["models"]["dbt_gold"]["staging"]["+materialized"]
        assert mat == "view", "Los modelos de staging deben materializarse como view"

    def test_materializacion_marts_es_table(self, dbt_project):
        mat = dbt_project["models"]["dbt_gold"]["marts"]["+materialized"]
        assert mat == "table", "Los marts deben materializarse como table"

    def test_profile_usa_spark_thrift(self, dbt_profile):
        conn = dbt_profile["dbt_gold"]["outputs"]["dev"]
        assert conn["type"] == "spark", "dbt debe conectarse via Spark"
        assert conn["method"] == "thrift", "El metodo de conexion debe ser thrift"
        assert conn["port"] == 10000, "El puerto del Thrift Server debe ser 10000"


# ---------------------------------------------------------------------------
# 5. Test de estructura del proyecto
# ---------------------------------------------------------------------------

class TestProjectStructure:
    """Verifica que los archivos necesarios del proyecto existen."""

    REQUIRED_FILES = [
        "docker-compose.yml",
        "dbt_gold/dbt_project.yml",
        "dbt_gold/profiles.yml",
        "dbt_gold/models/staging/stg_pharma_sales.sql",
        "dbt_gold/models/staging/_sources.yml",
        "dbt_gold/models/marts/_sources_ml.yml",
        "dbt_gold/models/marts/_schema.yml",
        "dbt_gold/models/marts/_schema_ml.yml",
        "airflow/dags/lakehouse_farma_pipeline.py",
        "src/spark/ingest_pharma.py",
        "src/spark/build_silver_pharma.py",
        "src/spark/build_gold.py",
        "src/spark/ml_customer_clustering.py",
        "src/spark/ml_demand_forecast.py",
    ]

    REQUIRED_MARTS = [
        "mart_cluster_analysis.sql",
        "mart_forecast_analysis.sql",
        "mart_geographic_analysis.sql",
        "mart_product_performance.sql",
        "mart_sales_rep_performance.sql",
    ]

    @pytest.mark.parametrize("filepath", REQUIRED_FILES)
    def test_archivo_requerido_existe(self, filepath):
        full_path = os.path.join(ROOT_DIR, filepath)
        assert os.path.exists(full_path), f"Falta: {filepath}"

    @pytest.mark.parametrize("mart", REQUIRED_MARTS)
    def test_mart_sql_existe(self, mart):
        full_path = os.path.join(ROOT_DIR, "dbt_gold", "models", "marts", mart)
        assert os.path.exists(full_path), f"Falta mart: {mart}"

    def test_csv_fuente_existe(self):
        csv_path = os.path.join(ROOT_DIR, "data", "landing", "pharma-data.csv")
        assert os.path.exists(csv_path), "No se encuentra pharma-data.csv en data/landing/"

    def test_csv_fuente_no_esta_vacio(self):
        csv_path = os.path.join(ROOT_DIR, "data", "landing", "pharma-data.csv")
        size = os.path.getsize(csv_path)
        assert size > 1_000_000, f"pharma-data.csv parece muy pequeño ({size} bytes)"


# ---------------------------------------------------------------------------
# 6. Test de configuracion Kafka con monkeypatch
# ---------------------------------------------------------------------------

class TestKafkaConfig:
    """
    Verifica la configuracion de Kafka usando monkeypatch
    para simular distintas variables de entorno.
    """

    def test_defaults_sin_env_vars(self):
        """Los valores por defecto deben apuntar al broker interno de Docker."""
        assert consumer_orders.KAFKA_BOOTSTRAP == "kafka:9093"
        assert consumer_orders.TOPIC == "orders"

    def test_override_bootstrap_con_monkeypatch(self, monkeypatch):
        """Simula un entorno con broker externo cambiando la variable."""
        monkeypatch.setattr(consumer_orders, "KAFKA_BOOTSTRAP", "broker-prod:9092")
        assert consumer_orders.KAFKA_BOOTSTRAP == "broker-prod:9092"

    def test_override_topic_con_monkeypatch(self, monkeypatch):
        """Simula un topic distinto para entorno de test."""
        monkeypatch.setattr(consumer_orders, "TOPIC", "orders_test")
        assert consumer_orders.TOPIC == "orders_test"

    def test_producer_defaults_consistentes(self):
        """El producer y el consumer deben apuntar al mismo broker y topic."""
        assert producer_module.BOOTSTRAP == consumer_orders.KAFKA_BOOTSTRAP
        assert producer_module.TOPIC == consumer_orders.TOPIC


# ---------------------------------------------------------------------------
# 7. Test de PharmaDataCatalog con mock CSV (fixture + monkeypatch)
# ---------------------------------------------------------------------------

# CSV de prueba minimo con las columnas que espera PharmaDataCatalog
MOCK_CSV_ROWS = [
    {
        "Product Name": "Aspirin 500mg",
        "Product Class": "Analgesic",
        "Price": "12.50",
        "Customer Name": "Farmacia Central",
        "City": "Madrid",
        "Country": "Spain",
        "Latitude": "40.4168",
        "Longitude": "-3.7038",
        "Channel": "Pharmacy",
        "Sub-channel": "Retail",
        "Name of Sales Rep": "Ana Lopez",
        "Manager": "Carlos Ruiz",
        "Sales Team": "Iberia",
        "Quantity": "100",
        "Sales": "1250.00",
    },
    {
        "Product Name": "Aspirin 500mg",
        "Product Class": "Analgesic",
        "Price": "14.00",
        "Customer Name": "Hospital Clinico",
        "City": "Barcelona",
        "Country": "Spain",
        "Latitude": "41.3851",
        "Longitude": "2.1734",
        "Channel": "Hospital",
        "Sub-channel": "Public",
        "Name of Sales Rep": "Pedro Garcia",
        "Manager": "Carlos Ruiz",
        "Sales Team": "Iberia",
        "Quantity": "500",
        "Sales": "7000.00",
    },
    {
        "Product Name": "Ibuprofen 400mg",
        "Product Class": "Anti-inflammatory",
        "Price": "8.75",
        "Customer Name": "Farmacia Central",
        "City": "Madrid",
        "Country": "Spain",
        "Latitude": "40.4168",
        "Longitude": "-3.7038",
        "Channel": "Pharmacy",
        "Sub-channel": "Retail",
        "Name of Sales Rep": "Ana Lopez",
        "Manager": "Carlos Ruiz",
        "Sales Team": "Iberia",
        "Quantity": "200",
        "Sales": "1750.00",
    },
]


@pytest.fixture
def mock_csv_path(tmp_path):
    """Crea un CSV temporal con datos de prueba (similar a pharma-data.csv)."""
    csv_file = tmp_path / "pharma-test.csv"
    fieldnames = MOCK_CSV_ROWS[0].keys()
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(MOCK_CSV_ROWS)
    return str(csv_file)


class TestPharmaDataCatalog:
    """Prueba la carga del catalogo desde CSV usando un fichero mock."""

    def test_carga_productos(self, mock_csv_path):
        catalog = producer_module.PharmaDataCatalog(mock_csv_path)
        names = [p["name"] for p in catalog.products]
        assert "Aspirin 500mg" in names
        assert "Ibuprofen 400mg" in names
        assert len(catalog.products) == 2, "Debe haber 2 productos distintos"

    def test_rango_precios_producto(self, mock_csv_path):
        catalog = producer_module.PharmaDataCatalog(mock_csv_path)
        aspirin = next(p for p in catalog.products if p["name"] == "Aspirin 500mg")
        assert aspirin["price_min"] == 12.50
        assert aspirin["price_max"] == 14.00

    def test_carga_clientes(self, mock_csv_path):
        catalog = producer_module.PharmaDataCatalog(mock_csv_path)
        names = [c["name"] for c in catalog.customers]
        assert "Farmacia Central" in names
        assert "Hospital Clinico" in names
        assert len(catalog.customers) == 2, "Debe haber 2 clientes distintos"

    def test_carga_paises(self, mock_csv_path):
        catalog = producer_module.PharmaDataCatalog(mock_csv_path)
        assert "Spain" in catalog.countries

    def test_carga_canales(self, mock_csv_path):
        catalog = producer_module.PharmaDataCatalog(mock_csv_path)
        assert "pharmacy" in catalog.channels
        assert "hospital" in catalog.channels

    def test_random_product_devuelve_producto_valido(self, mock_csv_path):
        catalog = producer_module.PharmaDataCatalog(mock_csv_path)
        product = catalog.random_product()
        assert "name" in product
        assert "class" in product
        assert "price_min" in product


# ---------------------------------------------------------------------------
# 8. Test de make_event con spy (mocker.spy)
# ---------------------------------------------------------------------------

class TestMakeEvent:
    """
    Verifica la generacion de eventos usando mocker.spy
    para comprobar que se usan los metodos del catalogo.
    """

    @pytest.fixture(autouse=True)
    def setup_catalog(self, mock_csv_path):
        """Inyecta un catalogo de prueba en el modulo producer."""
        producer_module.catalog = producer_module.PharmaDataCatalog(mock_csv_path)
        yield
        producer_module.catalog = None

    def test_evento_tiene_campos_del_schema(self):
        event = producer_module.make_event()
        for field in ["event_id", "order_id", "event_ts", "customer_name",
                       "city", "country", "product_name", "qty", "unit_price"]:
            assert field in event, f"Falta '{field}' en el evento generado"

    def test_evento_usa_producto_del_catalogo(self):
        event = producer_module.make_event()
        product_names = [p["name"] for p in producer_module.catalog.products]
        assert event["product_name"] in product_names

    def test_evento_usa_cliente_del_catalogo(self):
        event = producer_module.make_event()
        customer_names = [c["name"] for c in producer_module.catalog.customers]
        assert event["customer_name"] in customer_names

    def test_qty_es_positivo(self):
        event = producer_module.make_event()
        assert event["qty"] > 0

    def test_unit_price_es_positivo(self):
        event = producer_module.make_event()
        assert event["unit_price"] > 0

    def test_spy_random_product(self, mocker: MockerFixture):
        """Verifica con spy que make_event llama a random_product."""
        spy = mocker.spy(producer_module.catalog, "random_product")
        producer_module.make_event()
        assert spy.call_count == 1

    def test_spy_random_customer(self, mocker: MockerFixture):
        """Verifica con spy que make_event llama a random_customer."""
        spy = mocker.spy(producer_module.catalog, "random_customer")
        producer_module.make_event()
        assert spy.call_count == 1