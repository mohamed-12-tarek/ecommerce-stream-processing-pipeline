from ..ingestion.read_events import read_events
from ..session import create_spark_session


def main() -> None:
    spark = create_spark_session()

    df = read_events(spark)

    print("=== Schema ===")
    df.printSchema()

    print("=== Sample Data ===")
    df.show(20, truncate=False)

    print("=== Columns ===")
    print(df.columns)

    print("=== Number of Rows ===")
    print(df.count())

    spark.stop()


if __name__ == "__main__":
    main()