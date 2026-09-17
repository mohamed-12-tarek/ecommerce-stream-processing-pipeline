from pyflink.common import Configuration
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common.serialization import SimpleStringSchema


def main():
    config = Configuration()

    config.set_string(
        "pipeline.jars",
        "file:///home/mohamed/MyData/Tasks/FinalTask/"
        "flink/lib/flink-connector-kafka-5.0.0-2.2.jar;"
        "file:///home/mohamed/MyData/Tasks/FinalTask/"
        "flink/lib/kafka-clients-4.2.0.jar",
    )

    env = StreamExecutionEnvironment.get_execution_environment(
        configuration=config
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("ecommerce-events")
        .set_group_id("flink-test")
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source",
    )

    stream.print()

    env.execute("Kafka To Print")


if __name__ == "__main__":
    main()
