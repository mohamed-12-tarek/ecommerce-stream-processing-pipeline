import json
from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError
from hdfs import InsecureClient

from .config import (
    HDFS_BASE_PATH,
    HDFS_BATCH_SIZE,
    HDFS_CONSUMER_GROUP,
    HDFS_HEARTBEAT_INTERVAL_MS,
    HDFS_LOCALHOST,
    HDFS_MAX_POLL_INTERVAL_MS,
    HDFS_POLL_TIMEOUT_MS,
    HDFS_REQUEST_TIMEOUT_MS,
    HDFS_RETRY_BACKOFF_MS,
    HDFS_SESSION_TIMEOUT_MS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)
from .hdfs_writer import HDFSWriter


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=HDFS_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=HDFS_BATCH_SIZE,
        max_poll_interval_ms=HDFS_MAX_POLL_INTERVAL_MS,
        session_timeout_ms=HDFS_SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=HDFS_HEARTBEAT_INTERVAL_MS,
        request_timeout_ms=HDFS_REQUEST_TIMEOUT_MS,
        retry_backoff_ms=HDFS_RETRY_BACKOFF_MS,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key is not None else None,
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
            records = consumer.poll(
                timeout_ms=HDFS_POLL_TIMEOUT_MS,
                max_records=HDFS_BATCH_SIZE
            )

            if not records:
                continue

            events = [message.value for messages in records.values() for message in messages]

            if not events:
                continue

            # Write first, then commit the Kafka offsets. If the commit fails,
            # the batch may be delivered again, which is safer than losing data.
            writer.write_batch(events)

            try:
                consumer.commit()
            except KafkaTimeoutError as exc:
                print(
                    """Kafka offset commit timed out after the HDFS write.
                    The batch may be replayed on restart."""
                )
                raise exc

            print(f"Written {len(events)} events to HDFS.")

    except KeyboardInterrupt:
        print("\nStopping HDFS consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
