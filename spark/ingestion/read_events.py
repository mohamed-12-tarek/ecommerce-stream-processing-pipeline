from pyspark.sql import DataFrame, SparkSession
from ..config import HDFS_BASE_PATH


def read_events(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(HDFS_BASE_PATH)