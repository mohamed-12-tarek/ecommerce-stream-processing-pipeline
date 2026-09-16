import json
from kafka import KafkaConsumer
from .config import(
    HDFS_BATCH_SIZE,
    HDFS_CONSUMER_GROUP,
    HDFS_POLL_TIMEOUT_MS,
    HDFS_LOCALHOST,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    HDFS_BASE_PATH
)
from .hdfs_writer import HDFSWriter
from hdfs import InsecureClient

def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=HDFS_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=HDFS_BATCH_SIZE,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: (key.decode("utf-8") if key is not None else None)
    )


def main() -> None:
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    hdfs_client = InsecureClient(HDFS_LOCALHOST, user="mohamed")

    writer = HDFSWriter(base_path=HDFS_BASE_PATH, client=hdfs_client)

    print("HDFS Consumer started.")
    print(f"Group: {HDFS_CONSUMER_GROUP}")
    print(f"Topic: {KAFKA_TOPIC}")

    try:
        while True:
            records = consumer.poll(timeout_ms=HDFS_POLL_TIMEOUT_MS, max_records=HDFS_BATCH_SIZE)

            if not records:
                continue

            events = []

            for messages in records.values():
                for message in messages:
                    events.append(message.value)

            if not events:
                continue

            writer.write_batch(events)
            consumer.commit()

            print(f"Written {len(events)} events to HDFS.")

    except KeyboardInterrupt:
        print("\nStopping HDFS consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()