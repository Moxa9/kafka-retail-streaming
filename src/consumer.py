import json
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID
from src.db import connect, initialise_database
from src.events import validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def minute_window(event_time):
    if isinstance(event_time, str):
        event_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return event_time.replace(second=0, microsecond=0)


def persist_event(conn, event):
    validate_event(event)
    window_start = minute_window(event["event_time"])
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sales_events
                (event_id, event_time, product_id, category, quantity, unit_price, sales_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id""",
                (event["event_id"], event["event_time"], event["product_id"], event["category"],
                 int(event["quantity"]), event["unit_price"], event["sales_amount"])
            )
            inserted = cur.fetchone()
            if inserted:
                cur.execute(
                    """INSERT INTO sales_minute_aggregate
                    (window_start, category, event_count, units_sold, revenue)
                    VALUES (%s, %s, 1, %s, %s)
                    ON CONFLICT (window_start, category)
                    DO UPDATE SET
                      event_count = sales_minute_aggregate.event_count + 1,
                      units_sold = sales_minute_aggregate.units_sold + EXCLUDED.units_sold,
                      revenue = sales_minute_aggregate.revenue + EXCLUDED.revenue""",
                    (window_start, event["category"], int(event["quantity"]), event["sales_amount"])
                )
                return True
    return False


def main():
    initialise_database()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    conn = connect()
    logging.info("Consuming topic %s", KAFKA_TOPIC)
    try:
        for message in consumer:
            try:
                inserted = persist_event(conn, message.value)
                consumer.commit()
                logging.info("Processed event %s (new=%s)", message.value.get("event_id"), inserted)
            except Exception:
                logging.exception("Failed to process message; offset not committed")
                raise
    except KeyboardInterrupt:
        logging.info("Consumer stopping")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
