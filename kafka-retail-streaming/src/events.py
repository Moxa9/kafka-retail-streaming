from datetime import datetime, timezone
from uuid import uuid4

PRODUCTS = [
    ("P100", "Technology", 499.00),
    ("P200", "Furniture", 129.50),
    ("P300", "Office Supplies", 18.75),
    ("P400", "Accessories", 39.99),
]


def make_event(product=None, quantity=1, event_time=None):
    if product is None:
        product = PRODUCTS[0]
    product_id, category, unit_price = product
    return {
        "event_id": str(uuid4()),
        "event_time": (event_time or datetime.now(timezone.utc)).isoformat(),
        "product_id": product_id,
        "category": category,
        "quantity": int(quantity),
        "unit_price": float(unit_price),
        "sales_amount": round(int(quantity) * float(unit_price), 2),
    }


def validate_event(event):
    required = ["event_id", "event_time", "product_id", "category", "quantity", "unit_price", "sales_amount"]
    missing = [key for key in required if key not in event]
    if missing:
        raise ValueError(f"Missing event fields: {missing}")
    if int(event["quantity"]) <= 0:
        raise ValueError("quantity must be positive")
    if float(event["unit_price"]) < 0 or float(event["sales_amount"]) < 0:
        raise ValueError("prices and sales_amount must be non-negative")
    return event
