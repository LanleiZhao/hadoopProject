package GuliWeibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**https://www.cnblogs.com/LXL616/p/11028856.html
 * @author lucas
 * @create 2020-07-09-20:06
 */
public class WeiboUtil {
    private  static Configuration configuration = HBaseConfiguration.create();
    static {
        configuration.set("hbase.zookeeper.quorum","master");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
    }

    // 创建命名空间
    public static void createNamespace(String ns) throws IOException {
        // 创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        // 创建NS描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        // 创建操作
        admin.createNamespace(namespaceDescriptor);
        // 关闭资源
        admin.close();
        connection.close();
    }

    // 创建表
    public static void createTable(String tableName, int versons, String... cfs) throws IOException {
        // 创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        if (! admin.tableExists(TableName.valueOf(tableName))) {
            // 创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 循环添加列族
            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setMaxVersions(versons);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            // 创建表
            admin.createTable(hTableDescriptor);
        }
    }


}











