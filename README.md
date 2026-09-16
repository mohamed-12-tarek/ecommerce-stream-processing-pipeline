# E-Commerce Stream Processing Pipeline

An end-to-end e-commerce data platform that generates synthetic user events, streams them through Apache Kafka, stores the event stream in HDFS as Parquet, processes the same stream with Apache Flink for real-time workloads, and prepares the HDFS data for Apache Spark batch processing and machine learning.

## Architecture

```text
                    E-Commerce Data Generator
                              │
                              │ JSON Events
                              ▼
                         Apache Kafka
                              │
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
              Consumer Group A    Consumer Group B
                 hdfs-writer       realtime-pipeline
                    │                   │
                    ▼                   ▼
                   HDFS             Apache Flink
                    │                   │
                    │                   ▼
                    │                 HBase
                    │
                    ▼
               Apache Spark
                    │
          ┌─────────┴─────────┐
          │                   │
          ▼                   ▼
   Transformations      Feature Engineering
                              │
                              ▼
                       Machine Learning
```

## Components

- **Python** — synthetic e-commerce event generation and Kafka integration.
- **Apache Kafka** — durable event streaming and decoupling between consumers.
- **HDFS** — historical raw-event storage in Parquet format.
- **Apache Flink** — real-time stream processing path.
- **HBase** — low-latency serving/storage for the streaming path.
- **Apache Spark** — batch transformation and feature engineering over HDFS data.
- **Machine Learning** — downstream training using engineered features.
- **Docker Compose** — local Kafka environment.
- **uv** — Python dependency and environment management.

## Kafka Design

The topic is `ecommerce-events` with multiple partitions. The producer uses `user_id` as the Kafka message key, so events from the same user are routed consistently to the same partition.

Two independent consumer groups read the same topic:

- `hdfs-writer` consumes events for durable HDFS storage.
- `realtime-pipeline` is used by the Flink streaming path.

Because the groups are independent, consuming an event in one group does not advance the offsets of the other group.

## HDFS Consumer Reliability

The HDFS consumer uses manual offset commits:

1. Poll events from Kafka.
2. Write the batch to HDFS.
3. Commit Kafka offsets only after the HDFS write succeeds.

This favors **at-least-once delivery**: if the HDFS write succeeds but the Kafka commit fails, the same batch can be delivered again after restart. The downstream storage layer should therefore support deduplication when strict uniqueness is required.

Kafka consumer timeouts are configured to tolerate HDFS I/O latency and temporary broker/coordinator delays.

## Project Structure

```text
producer/
├── catalog.py
├── config.py
├── event_generator.py
├── hdfs_writer.py
├── kafka_consumer.py
├── kafka_producer.py
├── main.py
└── models.py

test/
└── test_generator.py

docker-compose.yml
pyproject.toml
uv.lock
```

## Prerequisites

- Python 3.12+
- uv
- Docker
- A running Hadoop/HDFS installation for the HDFS consumer
- Apache Flink for the streaming branch

## Setup

```bash
uv sync

docker compose up -d
```

Create the Kafka topic if it does not already exist:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic ecommerce-events \
  --partitions 3 \
  --replication-factor 1
```

## Run the Producer

```bash
uv run python -m producer.main
```

The producer continuously generates synthetic e-commerce sessions and publishes their events to Kafka.

## Run the HDFS Consumer

Make sure Hadoop/HDFS is running and the configured HDFS endpoint is reachable, then run:

```bash
uv run python -m producer.kafka_consumer
```

The consumer writes Parquet batches to:

```text
/mindmetrics/task5/data/ecommerce/events
```

## Check Kafka Consumer Group

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group hdfs-writer
```

The `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG` columns show the group's progress per partition.

## Data Flow

The HDFS path is the historical/batch branch:

```text
Kafka → HDFS (Parquet) → Spark → Feature Engineering → ML
```

The low-latency branch is independent:

```text
Kafka → Flink → HBase
```

This separation allows the same event stream to support both historical machine-learning workloads and real-time processing.

## Reliability Notes

The current HDFS consumer intentionally commits offsets **after** successful HDFS writes. This prevents acknowledging data that was never persisted, but it means a failed commit can produce duplicate HDFS records. A production implementation should add idempotent writes or event-level deduplication, preferably using `event_id` as the business identifier.

Malformed historical Kafka records from manual console-producer testing should not be mixed with the application's JSON event stream. For production hardening, a dead-letter/quarantine path can be added for invalid events.

## License

MIT
