package flink.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseConnectionCheck {
    public static void main(String[] args) throws Exception {
        var config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.setInt("hbase.zookeeper.property.clientPort", 2181);

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            boolean tableExists = connection.getAdmin().tableExists(
                    TableName.valueOf("ecommerce_events"));

            System.out.println("HBase connection: OK");
            System.out.println("ecommerce_events exists: " + tableExists);

            if (!tableExists) {
                throw new IllegalStateException(
                        "HBase table ecommerce_events does not exist");
            }
        }
    }
}
