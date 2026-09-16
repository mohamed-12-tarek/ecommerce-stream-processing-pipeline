import pyarrow as pa


EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("user_id", pa.int64()),
        ("session_id", pa.string()),
        ("product_id", pa.int64()),
        ("category", pa.string()),
        ("quantity", pa.int64()),
        ("price", pa.float64()),
        ("search_query", pa.string()),
        ("timestamp", pa.string()),
    ]
)
