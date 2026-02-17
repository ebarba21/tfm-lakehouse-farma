"""
Simulador de pedidos farmacéuticos para Kafka.

Genera eventos sintéticos CONSISTENTES con pharma-data.csv (mismos productos,
clientes, países, canales y rangos de precios) para permitir JOINs en Gold.

Uso: python producer.py
Env: KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, EMIT_INTERVAL_MS
"""

import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from collections import defaultdict

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
INTERVAL_MS = int(os.getenv("EMIT_INTERVAL_MS", "200"))

PHARMA_CSV_PATH = "/data/landing/pharma-data.csv"


class PharmaDataCatalog:
    """Catálogo de datos de pharma-data.csv para generar eventos consistentes."""

    def __init__(self, csv_path):
        self.products = []      # {name, class, price_min, price_max}
        self.customers = []     # {name, city, country, lat, lon, channel, sub_channel}
        self.channels = []
        self.sub_channels = defaultdict(list)
        self.countries = []
        self.sales_reps = []    # {name, manager, team}
        self._load_csv(csv_path)

    def _load_csv(self, csv_path):
        print(f"[producer] Cargando catálogo desde {csv_path}...")

        products_set = {}
        customers_set = {}
        channels_set = set()
        countries_set = set()
        sales_reps_set = {}

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Productos con rango de precios
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

                # Clientes
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

                channel = row["Channel"].strip().lower()
                sub_channel = row["Sub-channel"].strip().lower() if row.get("Sub-channel") else None
                channels_set.add(channel)
                if sub_channel:
                    self.sub_channels[channel].append(sub_channel)

                countries_set.add(row["Country"].strip())

                rep_name = row["Name of Sales Rep"].strip() if row.get("Name of Sales Rep") else None
                if rep_name and rep_name not in sales_reps_set:
                    sales_reps_set[rep_name] = {
                        "name": rep_name,
                        "manager": row.get("Manager", "").strip(),
                        "team": row.get("Sales Team", "").strip(),
                    }

        self.products = list(products_set.values())
        self.customers = list(customers_set.values())
        self.channels = list(channels_set)
        self.countries = list(countries_set)
        self.sales_reps = list(sales_reps_set.values())

        # Dedup sub_channels
        for ch in self.sub_channels:
            self.sub_channels[ch] = list(set(self.sub_channels[ch]))

        print(f"[producer] Catálogo cargado:")
        print(f"  - Productos: {len(self.products)}")
        print(f"  - Clientes/Sites: {len(self.customers)}")
        print(f"  - Países: {len(self.countries)} -> {self.countries}")
        print(f"  - Canales: {self.channels}")

    def random_product(self):
        return random.choice(self.products)

    def random_customer(self):
        return random.choice(self.customers)

    def random_sales_rep(self):
        return random.choice(self.sales_reps) if self.sales_reps else None


# Se carga una vez al inicio
catalog = None


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version_auto_timeout_ms=60000,
        request_timeout_ms=60000,
    )


def wait_for_kafka(max_wait_seconds=120):
    """Espera a que Kafka esté disponible, reintentando cada 3s."""
    start = time.time()
    last_err = None
    while time.time() - start < max_wait_seconds:
        try:
            p = build_producer()
            print(f"[producer] Conectado a Kafka en {BOOTSTRAP}")
            return p
        except NoBrokersAvailable as e:
            last_err = e
            print("[producer] Kafka no está listo aún. Reintentando en 3s...")
            time.sleep(3)
    raise RuntimeError(f"No se pudo conectar a Kafka tras {max_wait_seconds}s: {last_err}")


def make_event():
    """Genera evento de pedido sintético usando datos reales del catálogo."""
    global catalog

    product = catalog.random_product()
    customer = catalog.random_customer()

    # Precio dentro del rango real del producto (±10% variación)
    price_min = product["price_min"] * 0.9
    price_max = product["price_max"] * 1.1
    unit_price = round(random.uniform(max(1.0, price_min), max(2.0, price_max)), 2)

    # Distribución realista de cantidades (más pedidos pequeños)
    qty = random.choices(
        population=[1, 2, 3, 5, 10, 15, 20, 30, 50],
        weights=[25, 20, 15, 15, 10, 5, 5, 3, 2],
        k=1
    )[0]

    now = datetime.now(timezone.utc).isoformat()

    return {
        "event_id": fake.uuid4(),
        "order_id": fake.uuid4(),
        "event_ts": now,
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
        "latitude": customer.get("latitude"),
        "longitude": customer.get("longitude"),
    }


def main():
    global catalog

    catalog = PharmaDataCatalog(PHARMA_CSV_PATH)
    producer = wait_for_kafka()

    print(f"[producer] Iniciando emisión de eventos (intervalo: {INTERVAL_MS}ms)")
    print(f"[producer] Topic: {TOPIC}")

    event_count = 0

    while True:
        event = make_event()
        producer.send(TOPIC, event)
        producer.flush()

        event_count += 1
        if event_count % 100 == 0:
            print(f"[producer] Eventos emitidos: {event_count}")

        time.sleep(INTERVAL_MS / 1000.0)


if __name__ == "__main__":
    main()