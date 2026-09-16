import tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient

from .schema import EVENT_SCHEMA


class HDFSWriter:
    def __init__(self, base_path: str, client: InsecureClient) -> None:
        self.base_path = base_path.rstrip("/")
        self.client = client
        self.client.makedirs(self.base_path, permission="755")

    def write_batch(self, events: list[dict]) -> None:
        if not events:
            return

        table = self._events_to_table(events)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = Path(temp_file.name)

        try:
            pq.write_table(table, temp_path, compression="snappy")
            self._upload_to_hdfs(temp_path)
        finally:
            temp_path.unlink(missing_ok=True)

    @staticmethod
    def _events_to_table(events: list[dict]) -> pa.Table:
        """Convert a batch to the canonical event schema.

        The schema is explicit so every Parquet file written by this consumer
        has identical column types. This prevents schema drift between batches
        from breaking downstream Spark reads.
        """
        return pa.Table.from_pylist(events, schema=EVENT_SCHEMA)

    def _upload_to_hdfs(self, local_path: Path) -> None:
        remote_path = f"{self.base_path}/{local_path.name}"
        with local_path.open("rb") as file:
            self.client.write(remote_path, file, overwrite=False)
