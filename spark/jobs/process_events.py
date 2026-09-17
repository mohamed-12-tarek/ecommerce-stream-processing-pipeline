from pyspark.sql import functions as F

from ..config import PROCESSED_PATH
from ..ingestion.read_events import read_events
from ..session import create_spark_session
from ..transformations.clean_events import clean_events
from ..transformations.transform_events import transform_events


def main() -> None:
    spark = create_spark_session()

    try:
        # 1. Read raw events
        raw_df = read_events(spark)

        raw_count = raw_df.count()
        print(f"Raw rows: {raw_count}")

        # 2. Clean
        clean_df = clean_events(raw_df)

        clean_count = clean_df.count()
        print(f"Clean rows: {clean_count}")
        print(f"Removed rows: {raw_count - clean_count}")

        # 3. Transform
        transformed_df = transform_events(clean_df)

        # 4. Basic inspection
        print("\n=== Processed Schema ===")
        transformed_df.printSchema()

        print("\n=== Event Counts ===")
        (
            transformed_df
            .groupBy("event_type")
            .count()
            .orderBy(F.desc("count"))
            .show()
        )

        # 5. Write processed data
        (
            transformed_df
            .repartition("event_date")
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(PROCESSED_PATH)
        )

        print(f"\nProcessed data written to: {PROCESSED_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()