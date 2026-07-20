"""Generate deterministic mock data for the food-delivery batch pipeline.

Examples:
    python mock_data_generator.py --profile portfolio
    python mock_data_generator.py --orders 1000000 --customers 100000

CSV files represent MySQL source tables. ``menu_items.jsonl`` represents the
MongoDB collection and intentionally contains nested arrays for Spark to flatten.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from faker import Faker
except ImportError as exc:  # pragma: no cover - helpful CLI error
    raise SystemExit("Missing dependency. Install it with: python -m pip install Faker") from exc


PROFILES = {
    # Fast smoke test for CI/local development.
    "demo": dict(customers=1_000, restaurants=50, menu_per_restaurant=20, drivers=100, orders=10_000),
    # Large enough to demonstrate partitioning, joins and incremental loads.
    "portfolio": dict(customers=50_000, restaurants=500, menu_per_restaurant=30, drivers=2_000, orders=1_000_000),
    # Use only when benchmarking Spark; output is several GB.
    "stress": dict(customers=500_000, restaurants=5_000, menu_per_restaurant=40, drivers=20_000, orders=10_000_000),
}

CITIES = ["Bangkok", "Nonthaburi", "Pathum Thani", "Samut Prakan"]
RESTAURANT_CATEGORIES = ["Thai", "Japanese", "Korean", "Burger", "Dessert"]
MENU_CATEGORIES = ["Main", "Rice", "Noodle", "Drink", "Dessert"]
MENU_NAMES = [
    "Basil Chicken Rice", "Fried Rice", "Tom Yum Noodles", "Green Curry",
    "Chicken Burger", "Salmon Roll", "Kimchi Rice", "Pad Thai",
    "Thai Milk Tea", "Mango Sticky Rice", "Iced Coffee", "Cheesecake",
]
BASE_PRICES = [59, 69, 79, 89, 99, 129, 159, 199, 249]
ORDER_HOURS = [11, 12, 13, 18, 19, 20]
FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Peter",
    "Rose", "Sam", "Tom", "Zoe",
]
LAST_NAMES = [
    "Smith", "Jones", "Brown", "Taylor", "Wilson", "Davis", "Miller",
    "Clark", "Hall", "Young",
]
ROADS = [
    "Sukhumvit Road", "Silom Road", "Sathorn Road", "Ratchada Road",
    "Phahonyothin Road", "Chaeng Watthana Road", "Bang Na Road",
]

CSV_FIELDS = {
    "customers.csv": ["customer_id", "full_name", "email", "phone", "city", "created_at", "updated_at"],
    "restaurants.csv": ["restaurant_id", "restaurant_name", "category", "city", "address", "status", "created_at", "updated_at"],
    "orders.csv": ["order_id", "customer_id", "restaurant_id", "order_status", "subtotal", "discount", "delivery_fee", "total_amount", "ordered_at", "created_at", "updated_at"],
    "order_items.csv": ["order_item_id", "order_id", "menu_item_id", "menu_item_name", "quantity", "unit_price", "total_price", "created_at", "updated_at"],
    "payments.csv": ["payment_id", "order_id", "payment_method", "payment_status", "amount", "transaction_ref", "paid_at", "created_at", "updated_at"],
    "drivers.csv": ["driver_id", "driver_name", "number_plate", "driver_status", "created_at", "updated_at"],
    "deliveries.csv": ["delivery_id", "order_id", "driver_id", "delivery_status", "distance_km", "assigned_at", "picked_up_at", "delivered_at", "created_at", "updated_at"],
}


def iso(value: datetime | None) -> str:
    return "" if value is None else value.isoformat().replace("+00:00", "Z")


def random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=rng.randint(0, int((end - start).total_seconds())))


def open_csv(stack: ExitStack, path: Path, fields: list[str]) -> csv.DictWriter:
    handle = stack.enter_context(path.open("w", newline="", encoding="utf-8"))
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    return writer


def build_menu_document(menu_id: int, restaurant_id: int, rng: random.Random, updated_at: datetime) -> dict:
    name = f"{rng.choice(MENU_NAMES)} #{menu_id}"
    return {
        "_id": f"MENU-{menu_id:08d}",
        "restaurantId": restaurant_id,
        "name": name,
        "description": f"Mock menu item: {name}",
        "category": rng.choice(MENU_CATEGORIES),
        "basePrice": rng.choice(BASE_PRICES),
        "available": rng.random() < 0.95,
        "tags": rng.sample(["popular", "spicy", "vegetarian", "new", "value"], k=rng.randint(0, 2)),
        "options": [
            {
                "name": "Size",
                "required": True,
                "choices": [
                    {"name": "Regular", "extraPrice": 0},
                    {"name": "Large", "extraPrice": 40},
                ],
            }
        ],
        "createdAt": iso(updated_at - timedelta(days=rng.randint(30, 365))),
        "updatedAt": iso(updated_at),
    }


def maybe_dirty_text(value: str, rng: random.Random) -> str:
    """Return a common source-system formatting variation."""
    return rng.choice([value.lower(), value.upper(), f" {value}", f"{value} "])


def happens(rng: random.Random, base_rate: float, factor: float) -> bool:
    """Scale the CLI base rate for a particular source's realistic error rate."""
    return rng.random() < min(1.0, base_rate * factor)


def simple_english_name(entity_id: int) -> str:
    """Return readable, deterministic names such as Alice Smith and Bob Jones."""
    first = FIRST_NAMES[(entity_id - 1) % len(FIRST_NAMES)]
    last = LAST_NAMES[((entity_id - 1) // len(FIRST_NAMES)) % len(LAST_NAMES)]
    return f"{first} {last}"


def inject_menu_anomaly(doc: dict, rng: random.Random, anomalies: Counter) -> None:
    anomaly = rng.choices(
        ["category_format", "price_as_string", "missing_description"],
        weights=[35, 15, 50],
    )[0]
    if anomaly == "category_format":
        doc["category"] = maybe_dirty_text(doc["category"], rng)
    elif anomaly == "price_as_string":
        doc["basePrice"] = f"{doc['basePrice']}.00"
    elif anomaly == "missing_description":
        doc.pop("description", None)
    anomalies[f"menu.{anomaly}"] += 1


def generate(args: argparse.Namespace) -> None:
    config = PROFILES[args.profile].copy()
    for name in config:
        override = getattr(args, name)
        if override is not None:
            config[name] = override

    if any(value < 1 for value in config.values()):
        raise SystemExit("All row counts must be positive integers")

    rng = random.Random(args.seed)
    Faker.seed(args.seed)
    fake = Faker("th_TH")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    data_end = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)
    history_start = data_end - timedelta(days=730)
    order_start = data_end - timedelta(days=args.days - 1)
    restaurant_menus: dict[int, list[tuple[str, str, int]]] = {}
    anomalies: Counter = Counter()
    # A small group still emits a legacy MongoDB schema: clustered source-system
    # drift is more realistic than scattering renamed fields uniformly.
    legacy_restaurant_count = max(1, round(config["restaurants"] * 0.01))
    legacy_restaurant_ids = set(
        range(config["restaurants"] - legacy_restaurant_count + 1, config["restaurants"] + 1)
    )

    with ExitStack() as stack:
        writers = {
            filename: open_csv(stack, output_dir / filename, fields)
            for filename, fields in CSV_FIELDS.items()
        }
        menu_handle = stack.enter_context((output_dir / "menu_items.jsonl").open("w", encoding="utf-8"))

        for customer_id in range(1, config["customers"] + 1):
            created = random_datetime(rng, history_start, data_end - timedelta(days=30))
            customer = {
                "customer_id": customer_id, "full_name": simple_english_name(customer_id),
                "email": f"customer{customer_id}@example.com", "phone": fake.phone_number(),
                "city": rng.choice(CITIES), "created_at": iso(created), "updated_at": iso(created),
            }
            if happens(rng, args.dirty_rate, 1.4):
                anomaly = rng.choices(
                    ["missing_phone", "city_format", "name_whitespace"], weights=[30, 50, 20]
                )[0]
                if anomaly == "missing_phone":
                    customer["phone"] = ""
                elif anomaly == "city_format":
                    customer["city"] = maybe_dirty_text(customer["city"], rng)
                else:
                    customer["full_name"] = f"  {customer['full_name']} "
                anomalies[f"customer.{anomaly}"] += 1
            writers["customers.csv"].writerow(customer)

        for restaurant_id in range(1, config["restaurants"] + 1):
            created = random_datetime(rng, history_start, data_end - timedelta(days=90))
            restaurant_city = rng.choice(CITIES)
            restaurant = {
                "restaurant_id": restaurant_id, "restaurant_name": f"Restaurant {restaurant_id}",
                "category": rng.choice(RESTAURANT_CATEGORIES), "city": restaurant_city,
                "address": f"{rng.randint(1, 999)} {rng.choice(ROADS)}, {restaurant_city}",
                "status": rng.choices(["ACTIVE", "INACTIVE"], weights=[95, 5])[0],
                "created_at": iso(created), "updated_at": iso(created),
            }
            if happens(rng, args.dirty_rate, 0.8):
                anomaly = rng.choices(
                    ["category_format", "city_format", "status_format", "missing_address", "address_whitespace"],
                    weights=[30, 30, 10, 20, 10],
                )[0]
                if anomaly == "missing_address":
                    restaurant["address"] = ""
                elif anomaly == "address_whitespace":
                    restaurant["address"] = f"  {restaurant['address']} "
                else:
                    field = anomaly.split("_")[0]
                    restaurant[field] = maybe_dirty_text(restaurant[field], rng)
                anomalies[f"restaurant.{anomaly}"] += 1
            writers["restaurants.csv"].writerow(restaurant)
            restaurant_menus[restaurant_id] = []
            for offset in range(config["menu_per_restaurant"]):
                menu_id = (restaurant_id - 1) * config["menu_per_restaurant"] + offset + 1
                updated = random_datetime(rng, max(created, history_start), data_end)
                doc = build_menu_document(menu_id, restaurant_id, rng, updated)
                canonical_menu = (doc["_id"], doc["name"], doc["basePrice"])
                if args.dirty_rate > 0 and restaurant_id in legacy_restaurant_ids:
                    doc["base_price"] = doc.pop("basePrice")
                    anomalies["menu.legacy_field_cluster"] += 1
                elif happens(rng, args.dirty_rate, 1.2):
                    inject_menu_anomaly(doc, rng, anomalies)
                menu_handle.write(json.dumps(doc, ensure_ascii=False) + "\n")
                restaurant_menus[restaurant_id].append(canonical_menu)

        for driver_id in range(1, config["drivers"] + 1):
            created = random_datetime(rng, history_start, data_end - timedelta(days=30))
            driver = {
                "driver_id": driver_id, "driver_name": simple_english_name(driver_id + 7),
                "number_plate": f"BKK-{driver_id:05d}",
                "driver_status": rng.choices(["ACTIVE", "INACTIVE"], weights=[95, 5])[0],
                "created_at": iso(created), "updated_at": iso(created),
            }
            if happens(rng, args.dirty_rate, 0.6):
                anomaly = rng.choices(
                    ["driver_status_format", "missing_number_plate", "number_plate_format"],
                    weights=[50, 30, 20],
                )[0]
                if anomaly == "missing_number_plate":
                    driver["number_plate"] = ""
                elif anomaly == "number_plate_format":
                    driver["number_plate"] = maybe_dirty_text(driver["number_plate"], rng)
                else:
                    driver["driver_status"] = maybe_dirty_text(driver["driver_status"], rng)
                anomalies[f"driver.{anomaly}"] += 1
            writers["drivers.csv"].writerow(driver)

        order_item_id = payment_id = delivery_id = 1
        for order_id in range(1, config["orders"] + 1):
            restaurant_id = rng.randint(1, config["restaurants"])
            selected = rng.sample(restaurant_menus[restaurant_id], k=rng.randint(1, min(4, config["menu_per_restaurant"])))
            ordered_at = order_start + timedelta(
                days=rng.randint(0, args.days - 1), hours=rng.choice(ORDER_HOURS), minutes=rng.randint(0, 59)
            )
            subtotal = 0
            item_rows = []
            for menu_id, menu_name, price in selected:
                quantity = rng.randint(1, 3)
                total_price = quantity * price
                subtotal += total_price
                item = {
                    "order_item_id": order_item_id, "order_id": order_id,
                    "menu_item_id": menu_id, "menu_item_name": menu_name,
                    "quantity": quantity, "unit_price": price, "total_price": total_price,
                    "created_at": iso(ordered_at), "updated_at": iso(ordered_at),
                }
                if happens(rng, args.dirty_rate, 0.6):
                    anomaly = rng.choices(
                        ["unknown_menu", "total_mismatch", "name_whitespace"],
                        weights=[13, 7, 80],
                    )[0]
                    if anomaly == "unknown_menu":
                        # MySQL cannot enforce a reference to MongoDB; Spark must quarantine it.
                        item["menu_item_id"] = f"UNKNOWN-{order_item_id:08d}"
                    elif anomaly == "total_mismatch":
                        item["total_price"] = total_price + rng.choice([-10, 1, 10])
                    else:
                        item["menu_item_name"] = f" {menu_name}  "
                    anomalies[f"order_item.{anomaly}"] += 1
                item_rows.append(item)
                order_item_id += 1
            writers["order_items.csv"].writerows(item_rows)

            discount = rng.choice([0, 0, 0, 20, 30, 50])
            delivery_fee = rng.choice([20, 30, 40, 50])
            total_amount = subtotal - discount + delivery_fee
            status = rng.choices(["DELIVERED", "CANCELLED", "PREPARING"], weights=[85, 10, 5])[0]
            updated_at = ordered_at + timedelta(minutes=rng.randint(1, 75))
            order = {
                "order_id": order_id, "customer_id": rng.randint(1, config["customers"]),
                "restaurant_id": restaurant_id, "order_status": status,
                "subtotal": subtotal, "discount": discount, "delivery_fee": delivery_fee,
                "total_amount": total_amount, "ordered_at": iso(ordered_at),
                "created_at": iso(ordered_at), "updated_at": iso(updated_at),
            }
            if happens(rng, args.dirty_rate, 0.64):
                anomaly = rng.choices(
                    ["status_format", "total_mismatch", "late_update"],
                    weights=[34, 6, 60],
                )[0]
                if anomaly == "status_format":
                    order["order_status"] = maybe_dirty_text(status, rng)
                elif anomaly == "total_mismatch":
                    order["total_amount"] = total_amount + rng.choice([-10, 1, 10])
                else:
                    order["updated_at"] = iso(updated_at + timedelta(days=rng.randint(7, 30)))
                anomalies[f"order.{anomaly}"] += 1
            writers["orders.csv"].writerow(order)

            payment_status = "REFUNDED" if status == "CANCELLED" else ("PENDING" if status == "PREPARING" else "PAID")
            paid_at = None if payment_status == "PENDING" else ordered_at + timedelta(minutes=rng.randint(0, 3))
            payment = {
                "payment_id": payment_id, "order_id": order_id,
                "payment_method": rng.choice(["CARD", "PROMPTPAY", "CASH", "WALLET"]),
                "payment_status": payment_status, "amount": total_amount,
                "transaction_ref": f"TXN-{payment_id:012d}", "paid_at": iso(paid_at),
                "created_at": iso(ordered_at), "updated_at": iso(updated_at),
            }
            if happens(rng, args.dirty_rate, 0.54):
                anomaly = rng.choices(
                    ["method_format", "amount_mismatch", "missing_transaction_ref"],
                    weights=[75, 7, 18],
                )[0]
                if anomaly == "method_format":
                    payment["payment_method"] = maybe_dirty_text(payment["payment_method"], rng)
                elif anomaly == "amount_mismatch":
                    payment["amount"] = total_amount + rng.choice([-20, 10, 20])
                else:
                    payment["transaction_ref"] = ""
                anomalies[f"payment.{anomaly}"] += 1
            writers["payments.csv"].writerow(payment)
            payment_id += 1

            if status != "CANCELLED":
                assigned_at = ordered_at + timedelta(minutes=rng.randint(1, 8))
                picked_up_at = assigned_at + timedelta(minutes=rng.randint(8, 25))
                delivered_at = picked_up_at + timedelta(minutes=rng.randint(10, 45)) if status == "DELIVERED" else None
                delivery = {
                    "delivery_id": delivery_id, "order_id": order_id,
                    "driver_id": rng.randint(1, config["drivers"]),
                    "delivery_status": "DELIVERED" if status == "DELIVERED" else "ASSIGNED",
                    "distance_km": f"{rng.uniform(0.5, 18.0):.2f}", "assigned_at": iso(assigned_at),
                    "picked_up_at": iso(picked_up_at) if status == "DELIVERED" else "",
                    "delivered_at": iso(delivered_at), "created_at": iso(assigned_at),
                    "updated_at": iso(delivered_at or assigned_at),
                }
                if happens(rng, args.dirty_rate, 0.3):
                    anomaly = rng.choices(
                        ["status_format", "distance_outlier", "timestamp_order"],
                        weights=[73, 13, 14],
                    )[0]
                    if anomaly == "status_format":
                        delivery["delivery_status"] = maybe_dirty_text(delivery["delivery_status"], rng)
                    elif anomaly == "distance_outlier":
                        delivery["distance_km"] = rng.choice(["-1.00", "0.00", "999.00"])
                    else:
                        # A common clock/manual-entry issue: pickup appears before assignment.
                        delivery["picked_up_at"] = iso(assigned_at - timedelta(minutes=5))
                    anomalies[f"delivery.{anomaly}"] += 1
                writers["deliveries.csv"].writerow(delivery)
                delivery_id += 1

            if args.progress_every and order_id % args.progress_every == 0:
                print(f"Generated {order_id:,}/{config['orders']:,} orders")

    manifest = {
        "seed": args.seed,
        "profile": args.profile,
        "quality_model": "source_weighted_with_clustered_schema_drift_v1",
        "base_dirty_rate": args.dirty_rate,
        "intended_anomalies": dict(sorted(anomalies.items())),
        "note": "Counts describe deliberately injected issues; normal business nulls are not anomalies.",
    }
    with (output_dir / "generation_manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    print(f"Done: {output_dir.resolve()}")
    print(f"Profile={args.profile}, orders={config['orders']:,}, order_items={order_item_id - 1:,}")
    print(f"Base dirty rate={args.dirty_rate:.2%}, injected anomalies={sum(anomalies.values()):,}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", choices=PROFILES, default="demo")
    parser.add_argument("--output-dir", default="mock-data/output")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--end-date", default="2026-07-20", help="YYYY-MM-DD, inclusive")
    parser.add_argument("--days", type=int, default=180, help="Number of days containing orders")
    parser.add_argument("--progress-every", type=int, default=100_000, help="0 disables progress messages")
    parser.add_argument(
        "--dirty-rate", type=float, default=0.05,
        help="Base rate scaled by source-specific realistic weights (default: 0.05; use 0 for clean data)",
    )
    parser.add_argument("--customers", type=int)
    parser.add_argument("--restaurants", type=int)
    parser.add_argument("--menu-per-restaurant", dest="menu_per_restaurant", type=int)
    parser.add_argument("--drivers", type=int)
    parser.add_argument("--orders", type=int)
    args = parser.parse_args()
    try:
        datetime.fromisoformat(args.end_date)
    except ValueError as exc:
        parser.error(f"--end-date must be YYYY-MM-DD: {exc}")
    if args.days < 1:
        parser.error("--days must be positive")
    if not 0 <= args.dirty_rate <= 1:
        parser.error("--dirty-rate must be between 0 and 1")
    return args


if __name__ == "__main__":
    generate(parse_args())
