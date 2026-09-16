from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_user_activity(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_events"),
            
            F.sum("is_product_view").alias("product_views"),
            F.sum("is_add_to_cart").alias("add_to_carts"),
            F.sum("is_purchase").alias("purchases"),
            F.sum("is_search").alias("searches"),
            F.sum("revenue").alias("total_revenue"),

            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("session_id").alias("total_sessions"),
        )
    )
    
    
def aggregate_daily_activity(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy("event_date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("active_users"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.sum("is_product_view").alias("product_views"),
            F.sum("is_add_to_cart").alias("add_to_carts"),
            F.sum("is_purchase").alias("purchases"),
            F.sum("is_search").alias("searches"),
            F.sum("revenue").alias("total_revenue"),
        )
        .orderBy("event_date")
    )


def aggregate_category_activity(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy("category")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("product_id").alias("unique_products"),
            F.sum("is_product_view").alias("product_views"),
            F.sum("is_add_to_cart").alias("add_to_carts"),
            F.sum("is_purchase").alias("purchases"),
            F.sum("revenue").alias("total_revenue"),
        )
        .orderBy(F.desc("total_revenue"))
    )

