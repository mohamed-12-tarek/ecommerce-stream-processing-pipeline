# Flink → HBase

The project uses **Flink 2.2.1** for the streaming path and **HBase 2.5.15** as the real-time sink.

The previous HBase SQL connector artifact (`4.0.0-1.19`) was removed from the runtime path because it targets Flink 1.19, while this project runs Flink 2.2.1. The streaming job therefore uses a small Java `RichSinkFunction` backed directly by the HBase shaded client.

## Architecture

```text
Kafka: ecommerce-events
          |
          v
   Flink KafkaSource
          |
          v
 JsonRowDeserializationSchema
          |
          v
   HBaseEventSink (Java)
          |
          v
 HBase: ecommerce_events
```

## HBase table

The sink writes to:

- Table: `ecommerce_events`
- Column family: `event`
- Row key: `event_id`

Qualifiers:

`event_type`, `user_id`, `session_id`, `product_id`, `category`, `quantity`, `price`, `search_query`, `timestamp`

## Build the custom sink

```bash
cd ~/MyData/Tasks/FinalTask
chmod +x flink/build_hbase_sink.sh
./flink/build_hbase_sink.sh
```

The script compiles `flink/java/HBaseEventSink.java` and installs both the custom sink and the HBase shaded client into `FLINK_HOME/lib`, so the Py4J client classloader can construct the Java sink before job submission.

## Test HBase connectivity

Start HBase first, then run:

```bash
chmod +x flink/test_hbase.sh
./flink/test_hbase.sh
```

Expected output:

```text
HBase connection: OK
ecommerce_events exists: true
```

## Run the streaming job

Start Kafka and HBase, then run:

```bash
uv run python -m flink.kafka_to_hbase
```

The job uses consumer group `flink-hbase-sink` and starts at the latest Kafka offset. It remains running and writes newly arriving ecommerce events into HBase.

To verify rows from HBase Shell:

```bash
hbase shell
```

```hbase
count 'ecommerce_events'
scan 'ecommerce_events', {LIMIT => 5}
```

The sink is intentionally simple and provides at-least-once behavior: a Kafka record may be written again after a failure, but the HBase row key is `event_id`, so repeating the same event updates the same row instead of creating a duplicate row.
