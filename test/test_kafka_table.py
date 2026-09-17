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


config = Configuration()

config.set_string(
    "pipeline.jars",
    f"{KAFKA_CONNECTOR};"
    f"{KAFKA_CLIENT}",
)

settings = (
    EnvironmentSettings
    .new_instance()
    .in_streaming_mode()
    .with_configuration(config)
    .build()
)

table_env = TableEnvironment.create(settings)

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
        'properties.group.id' = 'flink-kafka-test',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
    """
)

print("Kafka Table API loaded successfully.")
