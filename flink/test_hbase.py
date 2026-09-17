import happybase


HOST = "localhost"
PORT = 9090
TABLE = b"ecommerce_events"


def main() -> None:
    connection = happybase.Connection(
        host=HOST,
        port=PORT,
        autoconnect=False,
    )
    connection.open()

    tables = connection.tables()
    print("HBase Thrift connection: OK")
    print("ecommerce_events exists:", TABLE in tables)

    if TABLE not in tables:
        raise RuntimeError(
            "HBase table ecommerce_events does not exist. "
            "Create it first with: create 'ecommerce_events', 'event'"
        )

    table = connection.table(TABLE)
    row_key = b"__flink_hbase_smoke_test__"
    table.put(row_key, {b"event:status": b"ok"})
    result = table.row(row_key)

    if result.get(b"event:status") != b"ok":
        raise RuntimeError("HBase write/read smoke test failed")

    table.delete(row_key)
    connection.close()
    print("HBase write/read/delete test: OK")


if __name__ == "__main__":
    main()
