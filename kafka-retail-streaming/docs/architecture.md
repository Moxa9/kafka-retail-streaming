# Architecture and processing semantics

1. Producer creates JSON sales events with UUID event IDs and sends them to Kafka keyed by product ID.
2. Consumer reads the `retail-sales` topic as a consumer group.
3. Consumer validates the payload, inserts it into `sales_events`, and updates a minute/category aggregate only if the event ID was new.
4. Kafka offset is committed after database persistence. If a message is replayed after a restart, the primary key prevents double counting.

This is at-least-once message delivery with idempotent database effects, not a claim of end-to-end exactly-once processing.
