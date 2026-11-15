"""
generate_sample_data.py
------------------------
Creates a realistic, slightly messy raw sales CSV so the ETL pipeline has
something real to extract, clean, and transform.

Run:
    python src/generate_sample_data.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "sales_data.csv"

PRODUCTS = [
    ("Wireless Mouse", "Electronics", 19.99),
    ("Mechanical Keyboard", "Electronics", 59.99),
    ("USB-C Hub", "Electronics", 29.99),
    ("Office Chair", "Furniture", 149.99),
    ("Standing Desk", "Furniture", 299.99),
    ("Desk Lamp", "Furniture", 24.99),
    ("Notebook Set", "Stationery", 8.99),
    ("Fountain Pen", "Stationery", 14.99),
    ("Sticky Notes Pack", "Stationery", 4.99),
    ("Bluetooth Speaker", "Electronics", 39.99),
    ("Laptop Stand", "Furniture", 34.99),
    ("Whiteboard", "Furniture", 44.99),
]

REGIONS = ["North", "South", "East", "West", "Central"]

CUSTOMERS = [f"Customer_{i:03d}" for i in range(1, 61)]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)


def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def maybe_dirty(value, dirty_chance=0.03):
    """Occasionally inject messy values so the ETL 'clean' step has real work to do."""
    if random.random() < dirty_chance:
        return ""  # missing value
    return value


def generate_rows(n_rows=6000):
    rows = []
    for i in range(1, n_rows + 1):
        product_name, category, base_price = random.choice(PRODUCTS)
        region = random.choice(REGIONS)
        customer = random.choice(CUSTOMERS)
        order_date = random_date(START_DATE, END_DATE)

        quantity = random.randint(1, 8)
        # small random price fluctuation (discounts/markups)
        unit_price = round(base_price * random.uniform(0.9, 1.15), 2)

        row = {
            "order_id": f"ORD{i:06d}",
            "order_date": order_date.strftime("%Y-%m-%d"),
            "product_name": maybe_dirty(product_name),
            "category": category,
            "region": maybe_dirty(region),
            "customer_name": customer,
            "quantity": maybe_dirty(quantity, dirty_chance=0.02),
            "unit_price": unit_price,
        }
        rows.append(row)

        # occasionally inject a duplicate row (real-world data quality issue)
        if random.random() < 0.01:
            rows.append(dict(row))

    return rows


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    rows = generate_rows()
    fieldnames = ["order_id", "order_date", "product_name", "category",
                  "region", "customer_name", "quantity", "unit_price"]

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
