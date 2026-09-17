import logging
from typing import Any

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.datastream import MapFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

from flink.hbase_writer import HBaseWriter


PROJECT_DIR = "/home/mohamed/MyData/Tasks/FinalTask"
KAFKA_CONNECTOR = (
    f"file://{PROJECT_DIR}/flink/lib/"
    "flink-connector-kafka-5.0.0-2.2.jar"
)
KAFKA_CLIENT = (
    f"file://{PROJECT_DIR}/flink/lib/"
    "kafka-clients-4.2.0.jar"
)

LOGGER = logging.getLogger(__name__)

ROW_TYPE = Types.ROW_NAMED(
    [
        "event_id",
        "event_type",
        "user_id",
        "session_id",
        "product_id",
        "category",
        "quantity",
        "price",
        "search_query",
        "timestamp",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.LONG(),
        Types.DOUBLE(),
        Types.STRING(),
        Types.STRING(),
    ],
)

JSON_DESERIALIZER = (
    JsonRowDeserializationSchema.builder()
    .type_info(ROW_TYPE)
    .build()
)


class HBaseMapFunction(MapFunction):
    """Write each Kafka event to HBase using the Python HappyBase client.

    HBase writes are keyed by event_id, making retries idempotent for the
    event row. The HBase Thrift server must be available on localhost:9090.
    """

    def __init__(self) -> None:
        self.writer: HBaseWriter | None = None

    def open(self, runtime_context: Any) -> None:
        self.writer = HBaseWriter(
            host="localhost",
            port=9090,
            table_name="ecommerce_events",
            column_family="event",
        )
        self.writer.connect()
        LOGGER.info("Connected to HBase Thrift on localhost:9090")

    def map(self, value: Any) -> Any:
        if self.writer is None:
            raise RuntimeError("HBase writer is not initialized")

        event = {
            "event_id": value[0],
            "event_type": value[1],
            "user_id": value[2],
            "session_id": value[3],
            "product_id": value[4],
            "category": value[5],
            "quantity": value[6],
            "price": value[7],
            "search_query": value[8],
            "timestamp": value[9],
        }
        self.writer.put_event(event)
        return value

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None


def main() -> None:
    config = Configuration()
    env = StreamExecutionEnvironment.get_execution_environment(
        configuration=config
    )
    env.set_parallelism(1)
    env.add_jars(KAFKA_CONNECTOR, KAFKA_CLIENT)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("ecommerce-events")
        .set_group_id("flink-hbase-sink")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(JSON_DESERIALIZER)
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Ecommerce Events",
    )

    # Python-only HBase integration. The final print is intentionally kept as
    # a visible terminal sink so the job has a concrete downstream operator.
    stream.map(
        HBaseMapFunction(),
        output_type=ROW_TYPE,
    ).name("Write Events To HBase").print()

    env.execute("Kafka To HBase")


if __name__ == "__main__":
    main()
