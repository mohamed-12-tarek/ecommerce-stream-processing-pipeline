from ..ingestion.read_events import read_events
from ..session import create_spark_session
from ..transformations.clean_events import clean_events
from ..transformations.transform_events import transform_events


def main() -> None:
    spark = create_spark_session()

    try:
        raw_df = read_events(spark)
        clean_df = clean_events(raw_df)
        transformed_df = transform_events(clean_df)

        print("Transformed Schema")
        transformed_df.printSchema()

        print("Transformed Data Sample")
        transformed_df.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
