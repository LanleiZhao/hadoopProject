package weibo.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import weibo.constants.Constant;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-10-14:33
 */
public class HBaseUtil {

    /**
     * 创建命名空间
     * @param nameSpace
     * @throws Exception
     */
    public static void createNameSpace(String nameSpace) throws Exception {
        // 获取连接
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        // 创建admin
        Admin admin = connection.getAdmin();
        // 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        // 创建命名空间
        admin.createNamespace(namespaceDescriptor);
        // 关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    private static boolean isTableExists(String tableName) throws IOException {
        // 获取连接
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        // 创建admin
        Admin admin = connection.getAdmin();
        // 判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }


    /**
     * 创建表
     * @param tableName
     * @param version
     * @param cfs
     */
    public static void createTable(String tableName, int version, String... cfs) throws IOException {
        // 1 判断是否传入了列族信息
        if (cfs.length == 0) {
            System.out.println("请传入列族信息");
            return;
        }

        // 2 判断表是否存在
        if (isTableExists(tableName)) {
            System.out.println(tableName+"表已经存在");
            return;
        }
        // 3 获取connection
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);

        // 4 获取admin
        Admin admin = connection.getAdmin();

        // 5 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 6 添加列族信息
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            // 7 设置版本
            hColumnDescriptor.setMaxVersions(version);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 8 创建表
        admin.createTable(hTableDescriptor);
        System.out.println(tableName+"表创建成功");

        // 9 释放资源
        admin.close();
        connection.close();

    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        if (!isTableExists(tableName)) {
            System.out.println(tableName+"表不存在");
          return;
        }
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(tableName);
        // 如果表处于禁用状态,直接删除
        if (admin.isTableDisabled(table)) {
            admin.deleteTable(table);
        }else{
            // 如果表在启用状态，则先禁用表，然后删除
            admin.disableTable(table);
            admin.deleteTable(table);
        }
        System.out.println(tableName+"删除成功");
        connection.close();
    }

}









