import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PRODUCER_INTERVAL_SECONDS
from src.events import make_event, PRODUCTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )
    logging.info("Producing events to topic %s", KAFKA_TOPIC)
    try:
        while True:
            import random
            event = make_event(product=random.choice(PRODUCTS), quantity=random.randint(1, 5))
            future = producer.send(KAFKA_TOPIC, key=event["product_id"], value=event)
            future.get(timeout=10)
            logging.info("Produced %s | %s | %.2f", event["event_id"], event["category"], event["sales_amount"])
            time.sleep(PRODUCER_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logging.info("Producer stopping")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
