from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


HISTORY_HOURS = 24
FUTURE_HOURS = 1
CUTOFF_INTERVAL_HOURS = 6


def generate_cutoffs(df: DataFrame) -> DataFrame:
    """
    Generate temporal cutoff points from the available event timeline.

    Each cutoff:
    - has at least 24 hours of historical data
    - has at least 1 hour of future data
    """

    bounds = df.agg(
        F.min("timestamp").alias("min_timestamp"),
        F.max("timestamp").alias("max_timestamp"),
    ).first()

    min_timestamp = bounds["min_timestamp"]
    max_timestamp = bounds["max_timestamp"]

    start = min_timestamp + timedelta(hours=HISTORY_HOURS)
    end = max_timestamp - timedelta(hours=FUTURE_HOURS)

    if start >= end:
        raise ValueError(
            "Not enough data to generate temporal training examples."
        )

    cutoffs = []
    current = start

    while current <= end:
        cutoffs.append((current,))
        current += timedelta(hours=CUTOFF_INTERVAL_HOURS)

    return df.sparkSession.createDataFrame(
        cutoffs,
        ["cutoff_time"],
    )


def build_lifetime_features(
    df: DataFrame,
    cutoffs: DataFrame,
) -> DataFrame:
    """
    Build cumulative user features using events before each cutoff.
    """

    historical = (
        df.alias("events")
        .crossJoin(F.broadcast(cutoffs).alias("cutoffs"))
        .filter(
            F.col("events.timestamp")
            < F.col("cutoffs.cutoff_time")
        )
    )

    return (
        historical
        .groupBy(
            "cutoffs.cutoff_time",
            "events.user_id",
        )
        .agg(
            F.count("*").alias("total_events"),

            F.sum("is_product_view").alias("product_views"),
            F.sum("is_add_to_cart").alias("add_to_carts"),
            F.sum("is_purchase").alias("purchases"),
            F.sum("is_search").alias("searches"),

            F.sum("revenue").alias("total_revenue"),

            F.countDistinct("session_id").alias(
                "number_of_sessions"
            ),

            F.countDistinct("product_id").alias(
                "unique_products"
            ),

            F.max("timestamp").alias(
                "last_event_time"
            ),

            F.max(
                F.when(
                    F.col("event_type") == "purchase",
                    F.col("timestamp"),
                )
            ).alias("last_purchase_time"),
        )
    )


def build_recent_features(
    df: DataFrame,
    cutoffs: DataFrame,
) -> DataFrame:
    """
    Build rolling 1-hour and 24-hour behavioral features.
    """

    historical = (
        df.alias("events")
        .crossJoin(F.broadcast(cutoffs).alias("cutoffs"))
        .filter(
            F.col("events.timestamp")
            < F.col("cutoffs.cutoff_time")
        )
    )

    return (
        historical
        .groupBy(
            "cutoffs.cutoff_time",
            "events.user_id",
        )
        .agg(
            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 1 HOUR"),
                    F.col("events.is_product_view"),
                ).otherwise(0)
            ).alias("views_last_1h"),

            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 24 HOURS"),
                    F.col("events.is_product_view"),
                ).otherwise(0)
            ).alias("views_last_24h"),

            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 1 HOUR"),
                    F.col("events.is_add_to_cart"),
                ).otherwise(0)
            ).alias("cart_adds_last_1h"),

            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 24 HOURS"),
                    F.col("events.is_add_to_cart"),
                ).otherwise(0)
            ).alias("cart_adds_last_24h"),

            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 1 HOUR"),
                    F.col("events.is_purchase"),
                ).otherwise(0)
            ).alias("purchases_last_1h"),

            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 24 HOURS"),
                    F.col("events.is_purchase"),
                ).otherwise(0)
            ).alias("purchases_last_24h"),

            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 1 HOUR"),
                    F.col("events.is_search"),
                ).otherwise(0)
            ).alias("searches_last_1h"),

            F.sum(
                F.when(
                    F.col("events.timestamp")
                    >= F.col("cutoffs.cutoff_time")
                    - F.expr("INTERVAL 24 HOURS"),
                    F.col("events.is_search"),
                ).otherwise(0)
            ).alias("searches_last_24h"),
        )
    )


def build_recency_features(
    df: DataFrame,
    cutoffs: DataFrame,
) -> DataFrame:
    """
    Calculate how long it has been since the user's last activity.
    """

    historical = (
        df.alias("events")
        .crossJoin(F.broadcast(cutoffs).alias("cutoffs"))
        .filter(
            F.col("events.timestamp")
            < F.col("cutoffs.cutoff_time")
        )
    )

    return (
        historical
        .groupBy(
            "cutoffs.cutoff_time",
            "events.user_id",
        )
        .agg(
            (
                F.unix_timestamp("cutoffs.cutoff_time")
                - F.unix_timestamp(F.max("events.timestamp"))
            ).alias("time_since_last_event_seconds"),

            (
                F.unix_timestamp("cutoffs.cutoff_time")
                - F.unix_timestamp(
                    F.max(
                        F.when(
                            F.col("events.event_type")
                            == "purchase",
                            F.col("events.timestamp"),
                        )
                    )
                )
            ).alias(
                "time_since_last_purchase_seconds"
            ),
        )
    )


def build_target(
    df: DataFrame,
    cutoffs: DataFrame,
) -> DataFrame:
    """
    Target = whether the user makes a purchase
    during the one-hour period after the cutoff.
    """

    future = (
        df.alias("events")
        .crossJoin(F.broadcast(cutoffs).alias("cutoffs"))
        .filter(
            (F.col("events.timestamp")
             >= F.col("cutoffs.cutoff_time"))
            &
            (F.col("events.timestamp")
             < F.col("cutoffs.cutoff_time")
             + F.expr("INTERVAL 1 HOUR"))
        )
        .filter(
            F.col("events.event_type") == "purchase"
        )
    )

    return (
        future
        .groupBy(
            "cutoffs.cutoff_time",
            "events.user_id",
        )
        .agg(
            F.lit(1).alias("future_purchase")
        )
    )


def build_training_dataset(
    df: DataFrame,
) -> DataFrame:
    """
    Build the complete ML-ready temporal dataset.
    """

    cutoffs = generate_cutoffs(df)

    lifetime = build_lifetime_features(
        df,
        cutoffs,
    )

    recent = build_recent_features(
        df,
        cutoffs,
    )

    recency = build_recency_features(
        df,
        cutoffs,
    )

    target = build_target(
        df,
        cutoffs,
    )

    dataset = (
        lifetime
        .join(
            recent,
            ["cutoff_time", "user_id"],
            "left",
        )
        .join(
            recency,
            ["cutoff_time", "user_id"],
            "left",
        )
        .join(
            target,
            ["cutoff_time", "user_id"],
            "left",
        )
        .fillna(
            {
                "future_purchase": 0,
                "views_last_1h": 0,
                "views_last_24h": 0,
                "cart_adds_last_1h": 0,
                "cart_adds_last_24h": 0,
                "purchases_last_1h": 0,
                "purchases_last_24h": 0,
                "searches_last_1h": 0,
                "searches_last_24h": 0,
                "time_since_last_purchase_seconds": -1,
            }
        )
    )

    return (
        dataset
        .withColumn(
            "time_since_last_event_minutes",
            F.col("time_since_last_event_seconds") / 60,
        )
        .withColumn(
            "time_since_last_purchase_minutes",
            F.col("time_since_last_purchase_seconds") / 60,
        )
        .drop(
            "time_since_last_event_seconds",
            "time_since_last_purchase_seconds",
        )
        .withColumn(
            "purchase_rate",
            F.when(
                F.col("total_events") > 0,
                F.col("purchases")
                / F.col("total_events"),
            ).otherwise(0.0),
        )
        .withColumn(
            "cart_to_view_rate",
            F.when(
                F.col("product_views") > 0,
                F.col("add_to_carts")
                / F.col("product_views"),
            ).otherwise(0.0),
        )
        .orderBy(
            "cutoff_time",
            "user_id",
        )
    )