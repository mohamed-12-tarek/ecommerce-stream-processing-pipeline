package flink.hbase;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Flink sink that writes ecommerce event rows directly with the HBase client.
 *
 * The input Row fields are:
 * event_id, event_type, user_id, session_id, product_id, category,
 * quantity, price, search_query, timestamp.
 */
public class HBaseEventSink extends RichSinkFunction<Row> {
    private final String zookeeperQuorum;
    private final int zookeeperPort;
    private final String tableName;
    private final String columnFamily;

    private transient Connection connection;
    private transient Table table;

    public HBaseEventSink(
            String zookeeperQuorum,
            int zookeeperPort,
            String tableName,
            String columnFamily) {
        this.zookeeperQuorum = zookeeperQuorum;
        this.zookeeperPort = zookeeperPort;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        var config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.setInt("hbase.zookeeper.property.clientPort", zookeeperPort);

        connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        String eventId = (String) row.getField(0);
        if (eventId == null || eventId.isBlank()) {
            return;
        }

        Put put = new Put(Bytes.toBytes(eventId));

        addString(put, "event_type", row.getField(1));
        addLong(put, "user_id", row.getField(2));
        addString(put, "session_id", row.getField(3));
        addLong(put, "product_id", row.getField(4));
        addString(put, "category", row.getField(5));
        addLong(put, "quantity", row.getField(6));
        addDouble(put, "price", row.getField(7));
        addString(put, "search_query", row.getField(8));
        addString(put, "timestamp", row.getField(9));

        table.put(put);
    }

    private void addString(Put put, String qualifier, Object value) {
        if (value != null) {
            put.addColumn(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(qualifier),
                    Bytes.toBytes(value.toString()));
        }
    }

    private void addLong(Put put, String qualifier, Object value) {
        if (value instanceof Number number) {
            put.addColumn(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(qualifier),
                    Bytes.toBytes(number.longValue()));
        }
    }

    private void addDouble(Put put, String qualifier, Object value) {
        if (value instanceof Number number) {
            put.addColumn(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(qualifier),
                    Bytes.toBytes(number.doubleValue()));
        }
    }

    @Override
    public void close() throws Exception {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
