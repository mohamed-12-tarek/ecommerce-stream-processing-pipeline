from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment


PROJECT_DIR = "/home/mohamed/MyData/Tasks/FinalTask"

KAFKA_CONNECTOR = (
    f"file://{PROJECT_DIR}/flink/lib/"
    "flink-connector-kafka-5.0.0-2.2.jar"
)

KAFKA_CLIENT = (
    f"file://{PROJECT_DIR}/flink/lib/"
    "kafka-clients-4.2.0.jar"
)


def main():
    # ---------------------------------------------------------
    # 1. Make Flink load the external connectors
    # ---------------------------------------------------------

    config = Configuration()

    config.set_string(
        "pipeline.jars",
        f"{KAFKA_CONNECTOR};"
        f"{KAFKA_CLIENT};"
    )

    settings = (
        EnvironmentSettings
        .new_instance()
        .in_streaming_mode()
        .with_configuration(config)
        .build()
    )

    table_env = TableEnvironment.create(settings)

    # ---------------------------------------------------------
    # 2. Kafka source
    # ---------------------------------------------------------

    table_env.execute_sql(
        """
        CREATE TABLE kafka_events (
            event_id STRING,
            event_type STRING,
            user_id BIGINT,
            session_id STRING,
            product_id BIGINT,
            category STRING,
            quantity BIGINT,
            price DOUBLE,
            search_query STRING,
            `timestamp` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ecommerce-events',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-hbase-sink',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
        """
    )

    # ---------------------------------------------------------
    # 3. HBase sink
    # ---------------------------------------------------------

    table_env.execute_sql(
        """
        CREATE TABLE hbase_events (
            event_id STRING,

            event ROW<
                event_type STRING,
                user_id BIGINT,
                session_id STRING,
                product_id BIGINT,
                category STRING,
                quantity BIGINT,
                price DOUBLE,
                search_query STRING,
                `timestamp` STRING
            >,

            PRIMARY KEY (event_id) NOT ENFORCED
        ) WITH (
            'connector' = 'hbase-2.2',
            'table-name' = 'ecommerce_events',
            'zookeeper.quorum' = 'localhost:2181',
            'sink.parallelism' = '1'
        )
        """
    )

    # ---------------------------------------------------------
    # 4. Kafka -> HBase
    # ---------------------------------------------------------

    result = table_env.execute_sql(
        """
        INSERT INTO hbase_events
        SELECT
            event_id,

            ROW(
                event_type,
                user_id,
                session_id,
                product_id,
                category,
                quantity,
                price,
                search_query,
                `timestamp`
            )

        FROM kafka_events
        """
    )

    result.wait()


if __name__ == "__main__":
    main()
