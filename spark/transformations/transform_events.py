from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_events(df: DataFrame) -> DataFrame:
    transformed_df = (
        df
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_year", F.year("timestamp"))
        .withColumn("event_month", F.month("timestamp"))
        .withColumn("event_day", F.dayofmonth("timestamp"))
        .withColumn("event_hour", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))
        
        .withColumn(
            "revenue",
            F.when(
                F.col("price").isNotNull()
                & F.col("quantity").isNotNull(),
                F.col("price") * F.col("quantity")
            ).otherwise(F.lit(0.0))
        )
        
        .withColumn(
            "is_product_view",
            F.when(F.col("event_type") == "product_view", 1)
            .otherwise(0)
        )

        .withColumn(
            "is_add_to_cart",
            F.when(F.col("event_type") == "add_to_cart", 1)
            .otherwise(0),
        )

        .withColumn(
            "is_purchase",
            F.when(F.col("event_type") == "purchase", 1)
            .otherwise(0)
        )

        .withColumn(
            "is_search",
            F.when(F.col("event_type") == "search", 1)
            .otherwise(0),
        )
    )

    return transformed_df
