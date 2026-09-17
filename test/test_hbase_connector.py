from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    config = Configuration()

    config.set_string(
        "pipeline.jars",
        "file:///home/mohamed/MyData/Tasks/FinalTask/"
        "flink/lib/flink-connector-hbase-2.2-4.0.0-1.19.jar",
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
                event_timestamp STRING
            >,
            PRIMARY KEY (event_id) NOT ENFORCED
        ) WITH (
            'connector' = 'hbase-2.2',
            'table-name' = 'ecommerce_events',
            'zookeeper.quorum' = 'localhost:2181'
        )
        """
    )

    print("HBase connector loaded successfully!")


if __name__ == "__main__":
    main()