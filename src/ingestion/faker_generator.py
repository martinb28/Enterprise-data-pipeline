"""
Bronze Layer — Synthetic Data Generator (backup / volume testing).

Uses Faker to generate realistic e-commerce data at scale.
Useful when Kaggle credentials are not available or to test with large volumes.
"""

import csv
import io
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from faker import Faker

from src.ingestion.storage_client import StorageClient
from src.utils.config import get_config
from src.utils.logger import get_logger, log_stage

logger = get_logger(__name__)

fake = Faker("pt_BR")  # Brazilian locale to match Olist dataset
Faker.seed(42)
random.seed(42)


class FakerGenerator:
    """
    Generates synthetic e-commerce data that mirrors the Olist schema.

    Generates the following tables:
    - customers (100k rows)
    - orders (100k rows)
    - order_items (200k rows)
    - order_payments (150k rows)
    - sellers (5k rows)
    - products (30k rows)
    - reviews (80k rows)
    """

    PRODUCT_CATEGORIES = [
        "electronics", "furniture", "clothing", "sports", "toys",
        "books", "beauty", "health", "automotive", "food",
        "garden", "tools", "pet_shop", "watches", "stationery",
    ]

    ORDER_STATUSES = [
        "delivered", "delivered", "delivered", "delivered",  # 80% delivered
        "shipped", "processing", "canceled", "unavailable",
    ]

    PAYMENT_TYPES = ["credit_card", "credit_card", "boleto", "voucher", "debit_card"]

    def __init__(self, num_customers: int = 10_000):
        self.config = get_config()
        self.storage = StorageClient()
        self.num_customers = num_customers
        self._customer_ids: list = []
        self._seller_ids: list = []
        self._product_ids: list = []
        self._order_ids: list = []

    def _generate_customers(self) -> Tuple[List, List]:
        """Generates customer records."""
        logger.info(f"Generating {self.num_customers:,} customers...")
        fieldnames = [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state"
        ]
        rows = []
        for _ in range(self.num_customers):
            cid = fake.uuid4()
            self._customer_ids.append(cid)
            rows.append({
                "customer_id": cid,
                "customer_unique_id": fake.uuid4(),
                "customer_zip_code_prefix": fake.postcode()[:5],
                "customer_city": fake.city(),
                "customer_state": fake.estado_sigla(),
            })
        return fieldnames, rows

    def _generate_sellers(self, num_sellers: int = 500) -> Tuple[List, List]:
        """Generates seller records."""
        logger.info(f"Generating {num_sellers:,} sellers...")
        fieldnames = [
            "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
        ]
        rows = []
        for _ in range(num_sellers):
            sid = fake.uuid4()
            self._seller_ids.append(sid)
            rows.append({
                "seller_id": sid,
                "seller_zip_code_prefix": fake.postcode()[:5],
                "seller_city": fake.city(),
                "seller_state": fake.estado_sigla(),
            })
        return fieldnames, rows

    def _generate_products(self, num_products: int = 2_000) -> Tuple[List, List]:
        """Generates product records."""
        logger.info(f"Generating {num_products:,} products...")
        fieldnames = [
            "product_id", "product_category_name", "product_name_length",
            "product_description_length", "product_photos_qty",
            "product_weight_g", "product_length_cm", "product_height_cm",
            "product_width_cm"
        ]
        rows = []
        for _ in range(num_products):
            pid = fake.uuid4()
            self._product_ids.append(pid)
            rows.append({
                "product_id": pid,
                "product_category_name": random.choice(self.PRODUCT_CATEGORIES),
                "product_name_length": random.randint(10, 60),
                "product_description_length": random.randint(100, 2000),
                "product_photos_qty": random.randint(1, 6),
                "product_weight_g": random.randint(100, 10000),
                "product_length_cm": random.randint(10, 80),
                "product_height_cm": random.randint(5, 50),
                "product_width_cm": random.randint(10, 80),
            })
        return fieldnames, rows

    def _generate_orders(self) -> Tuple[List, List]:
        """Generates order records (1.2x customers to allow repeat buyers)."""
        num_orders = int(self.num_customers * 1.2)
        logger.info(f"Generating {num_orders:,} orders...")
        fieldnames = [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        rows = []
        base_date = datetime(2017, 1, 1)

        for _ in range(num_orders):
            oid = fake.uuid4()
            self._order_ids.append(oid)
            purchase_dt = base_date + timedelta(
                days=random.randint(0, 730),  # 2 years of data
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            status = random.choice(self.ORDER_STATUSES)
            approved_dt = purchase_dt + timedelta(hours=random.randint(1, 48))
            carrier_dt = approved_dt + timedelta(days=random.randint(1, 5))
            delivered_dt = carrier_dt + timedelta(days=random.randint(1, 10))
            estimated_dt = purchase_dt + timedelta(days=random.randint(7, 30))

            rows.append({
                "order_id": oid,
                "customer_id": random.choice(self._customer_ids),
                "order_status": status,
                "order_purchase_timestamp": purchase_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "order_approved_at": approved_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "order_delivered_carrier_date": carrier_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "order_delivered_customer_date": (
                    delivered_dt.strftime("%Y-%m-%d %H:%M:%S")
                    if status == "delivered" else ""
                ),
                "order_estimated_delivery_date": estimated_dt.strftime("%Y-%m-%d %H:%M:%S"),
            })
        return fieldnames, rows

    def _generate_order_items(self) -> Tuple[List, List]:
        """Generates order line items (avg 1.5 items per order)."""
        num_items = int(len(self._order_ids) * 1.5)
        logger.info(f"Generating {num_items:,} order items...")
        fieldnames = [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value"
        ]
        rows = []
        for order_id in self._order_ids:
            num_line_items = random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
            for item_id in range(1, num_line_items + 1):
                rows.append({
                    "order_id": order_id,
                    "order_item_id": item_id,
                    "product_id": random.choice(self._product_ids),
                    "seller_id": random.choice(self._seller_ids),
                    "shipping_limit_date": (
                        datetime.now() + timedelta(days=random.randint(1, 30))
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "price": round(random.uniform(9.99, 999.99), 2),
                    "freight_value": round(random.uniform(5.0, 80.0), 2),
                })
        return fieldnames, rows

    def _generate_payments(self) -> Tuple[List, List]:
        """Generates payment records."""
        logger.info(f"Generating payments for {len(self._order_ids):,} orders...")
        fieldnames = [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value"
        ]
        rows = []
        for order_id in self._order_ids:
            rows.append({
                "order_id": order_id,
                "payment_sequential": 1,
                "payment_type": random.choice(self.PAYMENT_TYPES),
                "payment_installments": random.choices(
                    [1, 2, 3, 6, 12], weights=[40, 20, 15, 15, 10]
                )[0],
                "payment_value": round(random.uniform(20.0, 2000.0), 2),
            })
        return fieldnames, rows

    def _generate_reviews(self) -> Tuple[List, List]:
        """Generates customer review records."""
        num_reviews = int(len(self._order_ids) * 0.8)  # 80% of orders have reviews
        logger.info(f"Generating {num_reviews:,} reviews...")
        fieldnames = [
            "review_id", "order_id", "review_score",
            "review_creation_date", "review_answer_timestamp"
        ]
        rows = []
        sampled_orders = random.sample(self._order_ids, min(num_reviews, len(self._order_ids)))
        for order_id in sampled_orders:
            rows.append({
                "review_id": fake.uuid4(),
                "order_id": order_id,
                "review_score": random.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 30, 50])[0],
                "review_creation_date": (
                    datetime.now() - timedelta(days=random.randint(1, 365))
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "review_answer_timestamp": (
                    datetime.now() - timedelta(days=random.randint(0, 30))
                ).strftime("%Y-%m-%d %H:%M:%S"),
            })
        return fieldnames, rows

    def _to_csv_bytes(self, fieldnames: list, rows: list) -> bytes:
        """Converts a list of dicts to CSV bytes."""
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        return buffer.getvalue().encode("utf-8")

    def generate_and_upload(self, date_partition: Optional[str] = None) -> Dict[str, int]:
        """
        Generates all synthetic tables and uploads them to Bronze layer.

        Args:
            date_partition: Date string YYYY-MM-DD. Defaults to today.

        Returns:
            Statistics dict with rows generated per table.
        """
        from datetime import date
        if date_partition is None:
            date_partition = date.today().strftime("%Y-%m-%d")

        bucket = self.config["storage"]["buckets"]["bronze"]
        stats = {}

        with log_stage(logger, "Synthetic Data Generation & Bronze Upload"):
            tables = [
                ("olist_customers_dataset.csv", self._generate_customers),
                ("olist_sellers_dataset.csv", lambda: self._generate_sellers(500)),
                ("olist_products_dataset.csv", lambda: self._generate_products(2_000)),
                ("olist_orders_dataset.csv", self._generate_orders),
                ("olist_order_items_dataset.csv", self._generate_order_items),
                ("olist_order_payments_dataset.csv", self._generate_payments),
                ("olist_order_reviews_dataset.csv", self._generate_reviews),
            ]

            for filename, generator_fn in tables:
                fieldnames, rows = generator_fn()
                csv_bytes = self._to_csv_bytes(fieldnames, rows)
                key = f"olist/raw/{date_partition}/{filename}"
                self.storage.upload_bytes(csv_bytes, bucket, key)
                stats[filename] = len(rows)
                logger.info(f"✅ {filename}: {len(rows):,} rows uploaded")

        logger.info(f"🎉 Generation complete: {stats}")
        return stats


if __name__ == "__main__":
    gen = FakerGenerator(num_customers=10_000)
    result = gen.generate_and_upload()
    logger.info(f"Result: {result}")
