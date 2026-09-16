from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALID_EVENT_TYPES = [
    "session_start",
    "session_end",
    "product_view",
    "add_to_cart",
    "purchase",
    "search",
]


# Event	            Required fields

# session_start	    user_id, session_id
# session_end	    user_id, session_id
# product_view	    user_id, session_id, product_id, category
# add_to_cart	    user_id, session_id, product_id, category, quantity
# purchase	        user_id, session_id, product_id, category, quantity, price
# search	        user_id, session_id, category, search_query

def clean_events(df: DataFrame) -> DataFrame:
    cleaned_df = (
        df
        .withColumn(
            "timestamp",
            F.to_timestamp("timestamp"),
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("session_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .filter(F.col("quantity").isNull() | (F.col("quantity") > 0))
        .filter(F.col("price").isNull() | (F.col("price") > 0))
        
        .filter(
            (F.col("event_type") != "product_view")
            | (
                F.col("product_id").isNotNull()
                & F.col("category").isNotNull()
            )
        )

        .filter(
            (F.col("event_type") != "add_to_cart")
            | (
                F.col("product_id").isNotNull()
                & F.col("category").isNotNull()
                & F.col("quantity").isNotNull()
            )
        )

        .filter(
            (F.col("event_type") != "purchase")
            | (
                F.col("product_id").isNotNull()
                & F.col("category").isNotNull()
                & F.col("quantity").isNotNull()
                & F.col("price").isNotNull()
            )
        )

        .filter(
            (F.col("event_type") != "search")
            | (
                F.col("category").isNotNull()
                & F.col("search_query").isNotNull()
            )
        )

        .dropDuplicates(["event_id"])
    )

    return cleaned_df