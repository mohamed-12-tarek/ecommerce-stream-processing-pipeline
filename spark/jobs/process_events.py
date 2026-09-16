from pyspark.sql import functions as F

from ..ingestion.read_events import read_events
from ..session import create_spark_session
from ..transformations.clean_events import (
    VALID_EVENT_TYPES,
    clean_events,
)


def main() -> None:
    spark = create_spark_session()

    try:
        df = read_events(spark)
        raw_count = df.count()

        print(f"No.Rows [Raw Data]: {raw_count}")
        print("No of raws deleted for multiple reasons")

        missing_event_id = df.filter(F.col("event_id").isNull()).count()
        missing_user_id = df.filter(F.col("user_id").isNull()).count()
        missing_session_id = df.filter(F.col("session_id").isNull()).count()
        invalid_event_type = df.filter(~F.col("event_type").isin(VALID_EVENT_TYPES)).count()

        invalid_quantity = df.filter(
            F.col("quantity").isNotNull() 
            & (F.col("quantity") <= 0)
        ).count()

        invalid_price = df.filter(
            F.col("price").isNotNull()
            & (F.col("price") <= 0)
        ).count()

        duplicate_event_ids = (
            df
            .groupBy("event_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(f"Missing event_id: {missing_event_id}")
        print(f"Missing user_id: {missing_user_id}")
        print(f"Missing session_id: {missing_session_id}")
        print(f"Invalid event_type: {invalid_event_type}")
        print(f"Invalid quantity: {invalid_quantity}")
        print(f"Invalid price: {invalid_price}")
        print(f"Duplicate event_ids: {duplicate_event_ids}")

        clean_df = clean_events(df)
        clean_count = clean_df.count()
        removed_count = raw_count - clean_count

        print(f"\nRaw rows: {raw_count}")
        print(f"Clean rows: {clean_count}")
        print(f"Removed rows: {removed_count}\n")

        print("Clean Schema")
        clean_df.printSchema()

        print("\nClean Data Sample")
        clean_df.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
