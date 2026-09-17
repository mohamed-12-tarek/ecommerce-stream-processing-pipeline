from datetime import datetime, timedelta, timezone
from io import BytesIO
import pyarrow.parquet as pq
from hdfs import InsecureClient
from .config import HDFS_BASE_PATH, HDFS_LOCALHOST


def get_latest_event_timestamp() -> datetime | None:
    client = InsecureClient(HDFS_LOCALHOST, user="mohamed")

    if not client.status(HDFS_BASE_PATH, strict=False):
        return None

    latest_timestamp: datetime | None = None

    for filename in client.list(HDFS_BASE_PATH):
        if not filename.endswith(".parquet"):
            continue

        hdfs_path = f"{HDFS_BASE_PATH}/{filename}"

        with client.read(hdfs_path) as reader:
            parquet_data = reader.read()

        table = pq.read_table(BytesIO(parquet_data), columns=["timestamp"])

        for value in table["timestamp"].to_pylist():
            if value is None:
                continue

            timestamp = datetime.fromisoformat(value)

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            timestamp = timestamp.astimezone(timezone.utc)

            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp

    if latest_timestamp is None:
        return None

    return latest_timestamp + timedelta(minutes=1)