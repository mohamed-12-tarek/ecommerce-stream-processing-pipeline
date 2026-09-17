from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment


HBASE_CLIENT = (
    "file:///home/mohamed/hbase-2.5.15/lib/"
    "shaded-clients/hbase-shaded-client-2.5.15.jar"
)


def main():
    config = Configuration()

    config.set_string(
        "pipeline.jars",
        HBASE_CLIENT,
    )

    env = StreamExecutionEnvironment.get_execution_environment(
        configuration=config
    )

    stream = env.from_collection([1])

    def test_hbase(_):
        from org.apache.hadoop.hbase.client import ConnectionFactory

        print("ConnectionFactory loaded:", ConnectionFactory)
        return _

    stream.map(test_hbase)

    env.execute("HBase Client Class Test")


if __name__ == "__main__":
    main()