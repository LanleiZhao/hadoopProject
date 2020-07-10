package weibo.dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import weibo.constants.Constant;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-11-00:40
 */
public class testDeleteTable {
    public static void main(String[] args) throws IOException {
//        HBaseUtil.deleteTable(Constant.CONTENT_TABLE);
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        Admin admin = connection.getAdmin();
        admin.enableTable(TableName.valueOf(Constant.CONTENT_TABLE));
        admin.disableTable(TableName.valueOf(Constant.CONTENT_TABLE));
        System.out.println(admin.isTableDisabled(TableName.valueOf(Constant.CONTENT_TABLE)));
    }
}
