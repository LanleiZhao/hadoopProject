package HbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author lucas
 * @create 2020-07-09-16:22
 */
public class HBaseApi {
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    public static boolean isTableExist(String tableName) {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
            return admin.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     */
    public static void createTable(String tableName, String... columnFamily) {
        try {
            // 1 创建HBaseAdmin
            HBaseAdmin admin = new HBaseAdmin(conf);
            // 2 判断表是否存在，如果存在则退出，否则进入3
            if (isTableExist(tableName)) {
                System.out.println(tableName+"表已经存在，无法创建");
                System.exit(0);
            }
            // 3 创建表的属性
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 4 创建表的列族配置
            for (String cf : columnFamily) {
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            // 5 根据配置创建表
            admin.createTable(hTableDescriptor);
            System.out.println("表已经创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     * @param tableName
     */
    public static void deleteTable(String tableName) {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
            // 1 判断表是否存在，如果没有，退出
            if (! isTableExist(tableName)){
                System.out.println(tableName+"表不存在，无法删除");
                System.exit(0);
            }
            // 2 如果存在表，先禁用表，后删除
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println(tableName + "表已经删除");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 向表中插入数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param value
     */
    public static void addRowData(String tableName,String rowKey,String columnFamily,String columnName,String value){
        try {
            // 创建HTable对象
            HTable hTable = new HTable(conf, tableName);
            // 向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            // 向put对象中组装数据
            put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
            hTable.put(put);
            hTable.close();
            System.out.println("插入数据成功...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     */
    public static void deleteMultiRow(String tableName, String... rows) {
        try {
            HTable hTable = new HTable(conf, tableName);
            ArrayList<Delete> deletes = new ArrayList<>();
            for (String row : rows) {
                Delete delete = new Delete(Bytes.toBytes(row));
                deletes.add(delete);
            }
            hTable.delete(deletes);
            System.out.println("delete success...");
            hTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取所有数据
     * @param tableName
     */
    public static void getAllRows(String tableName) {
        try {
            HTable hTable = new HTable(conf, tableName);
            Scan scan = new Scan();
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+"\t");
                    System.out.print("columnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                    System.out.print("column:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                    System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取某一行数据
     * @param tableName
     * @param rowKey
     */
    public static void getRow(String tableName, String rowKey) {
        try {
            HTable hTable = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = hTable.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+"\t");
                System.out.print("columnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                System.out.print("column:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取某一行指定“列族:列”的数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     */
    public static void getRowQualifier(String tableName, String rowKey, String columnFamily, String column) {
        try {
            HTable hTable = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

            Result result = hTable.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+"\t");
                System.out.print("columnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                System.out.print("column:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
