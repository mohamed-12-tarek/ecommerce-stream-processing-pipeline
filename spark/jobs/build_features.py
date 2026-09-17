from ..config import FEATURES_PATH, PROCESSED_PATH
from ..features.build_features import build_training_dataset
from ..session import create_spark_session


def main() -> None:
    spark = create_spark_session()

    try:
        df = spark.read.parquet(PROCESSED_PATH)

        features_df = build_training_dataset(df)

        print("=== Feature Schema ===")
        features_df.printSchema()

        print("=== Feature Sample ===")
        features_df.show(
            20,
            truncate=False,
        )

        print("=== Dataset Statistics ===")
        print(
            f"Rows: {features_df.count()}"
        )

        print("=== Target Distribution ===")
        features_df.groupBy(
            "future_purchase"
        ).count().show()

        (
            features_df
            .write
            .mode("overwrite")
            .parquet(FEATURES_PATH)
        )

        print(
            f"Features written to: {FEATURES_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()