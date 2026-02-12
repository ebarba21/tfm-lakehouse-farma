"""
Simulador de eventos de pedidos para Kafka.

Este módulo genera eventos sintéticos de pedidos farmacéuticos CONSISTENTES
con los datos de pharma-data.csv. Usa los mismos productos, clientes, países,
canales y rangos de precios del dataset real.

Esto permite hacer JOINs en la capa Gold entre:
- Ventas históricas (pharma_sales)
- Pedidos en tiempo real (orders)

Ejemplo de uso:
    $ python producer.py

Variables de entorno:
    KAFKA_BOOTSTRAP_SERVERS: Dirección del broker de Kafka (default: kafka:9093)
    KAFKA_TOPIC: Nombre del topic donde publicar (default: orders)
    EMIT_INTERVAL_MS: Intervalo entre eventos en milisegundos (default: 200)
"""

import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Any
from collections import defaultdict

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake: Faker = Faker()

# Configuración Kafka
BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
TOPIC: str = os.getenv("KAFKA_TOPIC", "orders")
INTERVAL_MS: int = int(os.getenv("EMIT_INTERVAL_MS", "200"))

# Ruta al CSV de referencia (dentro del contenedor)
PHARMA_CSV_PATH: str = "/data/landing/pharma-data.csv"


class PharmaDataCatalog:
    """
    Catálogo de datos extraídos de pharma-data.csv para generar
    eventos consistentes.
    """
    
    def __init__(self, csv_path: str):
        """Carga y procesa el CSV de pharma-data."""
        self.products: list[dict] = []  # {name, class, price_min, price_max}
        self.customers: list[dict] = []  # {name, city, country, lat, lon, channel, sub_channel}
        self.channels: list[str] = []
        self.sub_channels: dict[str, list[str]] = defaultdict(list)
        self.countries: list[str] = []
        self.sales_reps: list[dict] = []  # {name, manager, team}
        
        self._load_csv(csv_path)
    
    def _load_csv(self, csv_path: str) -> None:
        """Lee el CSV y extrae valores únicos."""
        print(f"[producer] Cargando catálogo desde {csv_path}...")
        
        products_set = {}  # product_name -> {class, prices}
        customers_set = {}  # customer_name -> info
        channels_set = set()
        countries_set = set()
        sales_reps_set = {}
        
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Productos: guardamos rango de precios por producto
                prod_name = row["Product Name"].strip()
                price = float(row["Price"]) if row["Price"] else 0
                
                if prod_name not in products_set:
                    products_set[prod_name] = {
                        "name": prod_name,
                        "class": row["Product Class"].strip(),
                        "price_min": price,
                        "price_max": price,
                    }
                else:
                    products_set[prod_name]["price_min"] = min(products_set[prod_name]["price_min"], price)
                    products_set[prod_name]["price_max"] = max(products_set[prod_name]["price_max"], price)
                
                # Clientes (customer = site en streaming)
                cust_name = row["Customer Name"].strip()
                if cust_name not in customers_set:
                    customers_set[cust_name] = {
                        "name": cust_name,
                        "city": row["City"].strip(),
                        "country": row["Country"].strip(),
                        "latitude": float(row["Latitude"]) if row["Latitude"] else None,
                        "longitude": float(row["Longitude"]) if row["Longitude"] else None,
                        "channel": row["Channel"].strip().lower(),
                        "sub_channel": row["Sub-channel"].strip().lower() if row.get("Sub-channel") else None,
                    }
                
                # Canales
                channel = row["Channel"].strip().lower()
                sub_channel = row["Sub-channel"].strip().lower() if row.get("Sub-channel") else None
                channels_set.add(channel)
                if sub_channel:
                    self.sub_channels[channel].append(sub_channel)
                
                # Países
                countries_set.add(row["Country"].strip())
                
                # Sales reps
                rep_name = row["Name of Sales Rep"].strip() if row.get("Name of Sales Rep") else None
                if rep_name and rep_name not in sales_reps_set:
                    sales_reps_set[rep_name] = {
                        "name": rep_name,
                        "manager": row.get("Manager", "").strip(),
                        "team": row.get("Sales Team", "").strip(),
                    }
        
        # Convertir a listas
        self.products = list(products_set.values())
        self.customers = list(customers_set.values())
        self.channels = list(channels_set)
        self.countries = list(countries_set)
        self.sales_reps = list(sales_reps_set.values())
        
        # Deduplicar sub_channels
        for ch in self.sub_channels:
            self.sub_channels[ch] = list(set(self.sub_channels[ch]))
        
        print(f"[producer] Catálogo cargado:")
        print(f"  - Productos: {len(self.products)}")
        print(f"  - Clientes/Sites: {len(self.customers)}")
        print(f"  - Países: {len(self.countries)} -> {self.countries}")
        print(f"  - Canales: {self.channels}")
    
    def random_product(self) -> dict:
        """Devuelve un producto aleatorio."""
        return random.choice(self.products)
    
    def random_customer(self) -> dict:
        """Devuelve un cliente aleatorio."""
        return random.choice(self.customers)
    
    def random_sales_rep(self) -> dict:
        """Devuelve un sales rep aleatorio."""
        return random.choice(self.sales_reps) if self.sales_reps else None


# Catálogo global (se carga una vez al inicio)
catalog: PharmaDataCatalog | None = None


def build_producer() -> KafkaProducer:
    """Construye y configura una instancia de KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version_auto_timeout_ms=60000,
        request_timeout_ms=60000,
    )


def wait_for_kafka(max_wait_seconds: int = 120) -> KafkaProducer:
    """Espera a que Kafka esté disponible e intenta conectar repetidamente."""
    start: float = time.time()
    last_err: NoBrokersAvailable | None = None
    while time.time() - start < max_wait_seconds:
        try:
            p: KafkaProducer = build_producer()
            print(f"[producer] Conectado a Kafka en {BOOTSTRAP}")
            return p
        except NoBrokersAvailable as e:
            last_err = e
            print("[producer] Kafka no está listo aún. Reintentando en 3s...")
            time.sleep(3)
    raise RuntimeError(f"No se pudo conectar a Kafka tras {max_wait_seconds}s: {last_err}")


def make_event() -> dict[str, Any]:
    """
    Genera un evento de pedido sintético CONSISTENTE con pharma-data.
    
    Usa productos, clientes y países reales del CSV para permitir
    JOINs en la capa Gold.
    
    Returns:
        dict con la estructura del evento de pedido.
    """
    global catalog
    
    # Seleccionar producto y cliente aleatorios del catálogo real
    product = catalog.random_product()
    customer = catalog.random_customer()
    
    # Precio dentro del rango real del producto (con algo de variación)
    price_min = product["price_min"] * 0.9  # 10% menos
    price_max = product["price_max"] * 1.1  # 10% más
    unit_price = round(random.uniform(max(1.0, price_min), max(2.0, price_max)), 2)
    
    # Cantidad con distribución realista (más pedidos pequeños)
    qty = random.choices(
        population=[1, 2, 3, 5, 10, 15, 20, 30, 50],
        weights=[25, 20, 15, 15, 10, 5, 5, 3, 2],
        k=1
    )[0]
    
    now: str = datetime.now(timezone.utc).isoformat()
    
    return {
        "event_id": fake.uuid4(),
        "order_id": fake.uuid4(),
        "event_ts": now,
        # Datos consistentes con pharma-data
        "customer_name": customer["name"],
        "city": customer["city"],
        "country": customer["country"],
        "channel": customer["channel"],
        "sub_channel": customer.get("sub_channel"),
        "product_name": product["name"],
        "product_class": product["class"],
        "qty": qty,
        "unit_price": unit_price,
        "currency": "EUR",
        # Coordenadas para análisis geográfico
        "latitude": customer.get("latitude"),
        "longitude": customer.get("longitude"),
    }


def main() -> None:
    """
    Punto de entrada principal del simulador.
    
    Carga el catálogo de pharma-data, conecta a Kafka y genera
    eventos de pedidos consistentes.
    """
    global catalog
    
    # Cargar catálogo de productos/clientes reales
    catalog = PharmaDataCatalog(PHARMA_CSV_PATH)
    
    # Conectar a Kafka
    producer: KafkaProducer = wait_for_kafka()
    
    print(f"[producer] Iniciando emisión de eventos (intervalo: {INTERVAL_MS}ms)")
    print(f"[producer] Topic: {TOPIC}")
    
    event_count = 0
    
    while True:
        event: dict[str, Any] = make_event()
        producer.send(TOPIC, event)
        producer.flush()
        
        event_count += 1
        if event_count % 100 == 0:
            print(f"[producer] Eventos emitidos: {event_count}")
        
        time.sleep(INTERVAL_MS / 1000.0)


if __name__ == "__main__":
    main()