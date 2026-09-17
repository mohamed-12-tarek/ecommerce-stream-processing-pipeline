import json
import logging
from typing import Any

import happybase

LOGGER = logging.getLogger(__name__)


class HBaseWriter:
    """Small Python client for writing ecommerce events through HBase Thrift."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9090,
        table_name: str = "ecommerce_events",
        column_family: str = "event",
    ) -> None:
        self.host = host
        self.port = port
        self.table_name = table_name.encode()
        self.column_family = column_family
        self.connection: happybase.Connection | None = None
        self.table: Any = None

    def connect(self) -> None:
        self.connection = happybase.Connection(
            host=self.host,
            port=self.port,
            autoconnect=False,
        )
        self.connection.open()
        self.table = self.connection.table(self.table_name)

    def put_event(self, event: dict[str, Any]) -> None:
        if self.table is None:
            self.connect()

        event_id = str(event["event_id"])
        prefix = f"{self.column_family}:"
        row = {
            f"{prefix}event_type": str(event.get("event_type", "")),
            f"{prefix}user_id": str(event.get("user_id", "")),
            f"{prefix}session_id": str(event.get("session_id", "")),
            f"{prefix}product_id": str(event.get("product_id", "")),
            f"{prefix}category": str(event.get("category", "")),
            f"{prefix}quantity": str(event.get("quantity", "")),
            f"{prefix}price": str(event.get("price", "")),
            f"{prefix}search_query": str(event.get("search_query", "")),
            f"{prefix}timestamp": str(event.get("timestamp", "")),
        }
        self.table.put(event_id, row)

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self.table = None


def write_json_line(writer: HBaseWriter, value: str) -> None:
    event = json.loads(value)
    writer.put_event(event)
