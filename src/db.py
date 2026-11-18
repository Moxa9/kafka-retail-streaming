import psycopg2
from src.config import POSTGRES

DDL = """
CREATE TABLE IF NOT EXISTS sales_events (
    event_id TEXT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    product_id TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL,
    sales_amount NUMERIC(14,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS sales_minute_aggregate (
    window_start TIMESTAMPTZ NOT NULL,
    category TEXT NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    units_sold BIGINT NOT NULL DEFAULT 0,
    revenue NUMERIC(16,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, category)
);
"""


def connect():
    return psycopg2.connect(**POSTGRES)


def initialise_database():
    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
    finally:
        conn.close()
