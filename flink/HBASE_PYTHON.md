# Python-only Flink -> HBase

This project intentionally does not use Java application code for the Flink/HBase path.

## Architecture

```text
Kafka -> PyFlink KafkaSource -> Python MapFunction -> HappyBase -> HBase Thrift -> HBase
```

The Python `MapFunction` opens one HappyBase connection per Python operator instance in `open()`, writes each event using `event_id` as the HBase row key, and closes the connection in `close()`.

## One-time setup

Install the Python dependency:

```bash
uv sync
```

Make sure the HBase table exists:

```bash
hbase shell
```

```text
create 'ecommerce_events', 'event'
```

Start the HBase Thrift server on port 9090. With the HBase installation used by this project:

```bash
$HBASE_HOME/bin/hbase thrift start -p 9090
```

Keep that process running in its own terminal.

## Test HBase first

From the project root:

```bash
uv run python -m flink.test_hbase
```

Expected:

```text
HBase Thrift connection: OK
ecommerce_events exists: True
HBase write/read/delete test: OK
```

## Run Flink -> HBase

Start Kafka first, then run:

```bash
uv run python -m flink.kafka_to_hbase
```

The job consumes `ecommerce-events` with consumer group `flink-hbase-sink` and writes every event to HBase.

Check the rows with:

```bash
hbase shell
```

```text
count 'ecommerce_events'
scan 'ecommerce_events', {LIMIT => 5}
```

## Important behavior

The HBase row key is `event_id`. HBase `Put` is therefore naturally idempotent for repeated delivery of the same event ID: a retry updates the same row instead of creating another row.

The Python MapFunction performs an external side effect. This keeps the whole project Python-only and is appropriate for this project/demo. For a production system, the HBase write should be implemented as a proper Flink Sink with checkpoint-aware delivery semantics.
