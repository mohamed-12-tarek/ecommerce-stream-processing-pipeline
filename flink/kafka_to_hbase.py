from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import SinkFunction
from pyflink.java_gateway import get_gateway


PROJECT_DIR = "/home/mohamed/MyData/Tasks/FinalTask"

KAFKA_CONNECTOR = (
    f"file://{PROJECT_DIR}/flink/lib/"
    "flink-connector-kafka-5.0.0-2.2.jar"
)

KAFKA_CLIENT = (
    f"file://{PROJECT_DIR}/flink/lib/"
    "kafka-clients-4.2.0.jar"
)


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


def main() -> None:
    config = Configuration()

    env = StreamExecutionEnvironment.get_execution_environment(
        configuration=config
    )
    env.set_parallelism(1)

    env.add_jars(
        KAFKA_CONNECTOR,
        KAFKA_CLIENT,
    )

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

    # The custom sink is installed into FLINK_HOME/lib by
    # build_hbase_sink.sh so Py4J can construct it before job submission.
    gateway = get_gateway()
    sink_class = gateway.jvm.flink.hbase.HBaseEventSink
    hbase_sink = SinkFunction(
        sink_class(
            "localhost",
            2181,
            "ecommerce_events",
            "event",
        )
    )

    stream.add_sink(hbase_sink).name("HBase Ecommerce Events Sink")

    env.execute("Kafka To HBase")


if __name__ == "__main__":
    main()
