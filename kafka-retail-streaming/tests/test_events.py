import pytest
from src.events import make_event, validate_event, PRODUCTS
from src.consumer import minute_window
from datetime import datetime, timezone


def test_make_event_has_expected_fields():
    event = make_event(product=PRODUCTS[0], quantity=2)
    assert validate_event(event) is event
    assert event["quantity"] == 2
    assert event["sales_amount"] == event["unit_price"] * 2


def test_validate_rejects_zero_quantity():
    event = make_event(product=PRODUCTS[0], quantity=1)
    event["quantity"] = 0
    with pytest.raises(ValueError):
        validate_event(event)


def test_minute_window_truncates_seconds():
    value = datetime(2025, 1, 2, 3, 4, 59, tzinfo=timezone.utc)
    assert minute_window(value) == datetime(2025, 1, 2, 3, 4, 0, tzinfo=timezone.utc)
