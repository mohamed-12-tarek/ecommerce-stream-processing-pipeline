from ..ingestion.read_events import read_events
from ..session import create_spark_session
from ..transformations.clean_events import clean_events
from ..transformations.transform_events import transform_events
from ..transformations.aggregations import aggregate_user_activity


def main() -> None:
    spark = create_spark_session()

    try:
        raw_df = read_events(spark)
        clean_df = clean_events(raw_df)
        transformed_df = transform_events(clean_df)
        user_df = aggregate_user_activity(transformed_df)

        print("User Activity Schema")
        user_df.printSchema()

        print("User Activity")
        user_df.show(20, truncate=False)

        print("Number of Users")
        print(user_df.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()