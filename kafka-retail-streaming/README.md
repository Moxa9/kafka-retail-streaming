# Kafka Retail Sales Streaming Mini-Project

A small local streaming pipeline: Python producer -> Kafka topic -> Python consumer -> PostgreSQL event store and minute-level category aggregates. It demonstrates event serialization, consumer groups, offset commits, idempotent event ingestion and incremental aggregation.

## Architecture

`Producer (Python) -> Kafka topic retail-sales -> Consumer (Python) -> PostgreSQL`

The consumer stores each event once using `event_id` as a primary key. It updates `sales_minute_aggregate` only when a new event is inserted, so replayed messages do not double-count revenue.

## Requirements

- Python 3.11+
- Apache Kafka running locally in KRaft mode (or another Kafka-compatible broker)
- PostgreSQL running locally
- No Docker is required, but Kafka and PostgreSQL must be installed and running.

## 1. Python setup (PowerShell)

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and set your PostgreSQL password and connection settings.

## 2. Start Kafka

Install an Apache Kafka binary distribution from the official Apache Kafka downloads page. For a local single-node development broker, use the KRaft quickstart instructions for your Kafka version. Ensure the broker listens on `localhost:9092`.

Create the topic (from the Kafka installation directory; Windows uses `.bat` scripts):

```powershell
.in\windows\kafka-topics.bat --create --topic retail-sales --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

If the topic already exists, Kafka reports that; continue.

## 3. Start PostgreSQL

Create a database named `retail_stream` in PostgreSQL, or change `POSTGRES_DB` in `.env`. The consumer creates the two required tables automatically. You can also run `sql/schema.sql` manually.

## 4. Run the pipeline (three terminals)

Terminal 1 — consumer:

```powershell
python -m src.consumer
```

Terminal 2 — producer:

```powershell
python -m src.producer
```

Terminal 3 — inspect the aggregate:

```powershell
python -m src.report
```

Stop producer/consumer with `Ctrl+C`.

## Example SQL

```sql
SELECT window_start, category, event_count, units_sold, revenue
FROM sales_minute_aggregate
ORDER BY window_start DESC, revenue DESC;
```

## Tests

```powershell
python -m pytest -v
```

Unit tests cover event construction, validation and time-window bucketing. End-to-end execution requires Kafka and PostgreSQL services to be running.

## Scope and limitations

This is an educational, single-broker/single-consumer-group demonstration, not a highly available production deployment. Aggregation uses event-time minute buckets but does not implement late-event correction, watermarking, schema registry, dead-letter queues, or exactly-once Kafka transactions. PostgreSQL event deduplication makes the database write idempotent for repeated event IDs.
