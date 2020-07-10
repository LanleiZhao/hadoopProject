package weibo.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import weibo.constants.Constant;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author lucas
 * @create 2020-07-10-14:58
 * <p>
 * 发布微博
 * 删除微博
 * 关注内容
 * 取关用户
 * 获取用户微博详情
 * 获取用户的初始化页面
 */
public class HBaseDao {

    /**
     * 发布微博
     *
     * @param uid
     * @param content
     */
    public static void publishWeibo(String uid, String content) throws IOException {
        // 获取连接
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        // Part1
        // 1 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
        // 2 获取当前时间戳
        long currentTimeMillis = System.currentTimeMillis();
        long ts = Long.MAX_VALUE - currentTimeMillis;
        // 3 获取Rowkey,按照时间倒序的方式排列，最新的微博在放在前面
        String rowKey = uid + "_" + ts;
        // 4 创建put对象
        Put contentPut = new Put(Bytes.toBytes(rowKey));
        // 5 给put对象赋值
        contentPut.addColumn(Bytes.toBytes(Constant.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));
        // 6 执行插入数据操作
        contentTable.put(contentPut);

        // Part2
        // 向微博收件箱表中加入发布的Rowkey
        // 1 获取当前uid的所有fans
        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));
        Get relationGet = new Get(Bytes.toBytes(uid));
        relationGet.addFamily(Bytes.toBytes(Constant.RELATION_CF2));
        Result result = relationTable.get(relationGet);

        // 2 遍历fans,创建put对象，封装到List，put对象的格式应该和inbox相同
        ArrayList<Put> inboxPutList = new ArrayList<>();
        Cell[] rawCells = result.rawCells();
        for (Cell cell : rawCells) {
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            Put inboxPut = new Put(qualifier);
            //插入InboxTable的put: RowKey是粉丝id，列族是info,列是偶像id,值是偶像的微博RowKey
            inboxPut.addColumn(Bytes.toBytes(Constant.INBOX_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inboxPutList.add(inboxPut);
        }

        // 3 判断list的长度，如果大于0，获取inboxTable连接，插入list
        if (inboxPutList.size() > 0) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
            inboxTable.put(inboxPutList);
            inboxTable.close();
        }

        // 4 释放资源
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * a、在微博用户关系表中，对当前主动操作的用户添加新关注的好友
     * b、在微博用户关系表中，对被关注的用户添加新的粉丝
     * c、微博收件箱表中添加所关注的用户发布的微博
     * @param uid
     * @param attends
     */
    public static void addAttends(String uid, String... attends) throws IOException {
        // part1:操作用户关系表
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        // 1 获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));
        // 2 创建一个集合，用于存放Put对象
        ArrayList<Put> putsList = new ArrayList<>();
        // 3 创建操作者的Put，属性赋值，添加到集合
        Put uPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            uPut.addColumn(Bytes.toBytes(Constant.RELATION_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));
        }
        putsList.add(uPut);

        // 4 循环创建被关注者的Put对象，添加到集合
        for (String attend : attends) {
            Put put = new Put(Bytes.toBytes(attend));
            put.addColumn(Bytes.toBytes(Constant.RELATION_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            putsList.add(put);
        }
        // 5 插入数据
        relationTable.put(putsList);


        // part2:操作收件箱表对象
        // 1 创建内容表的描述对象
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
        // 2 创建收件箱表的Put对象
        Put inBoxPut = new Put(Bytes.toBytes(uid));
        // 3 循环attends,获取每个被关注者近期发布的微博
        for (String attend : attends) {
            // 4 获取当前被关注者近期发布的微博-》集合
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contentTable.getScanner(scan);
            // 定义一个时间戳
            long ts = System.currentTimeMillis();
            // 5 对获取的值进行遍历
            for (Result result : resultScanner) {
                // 6 给收件箱表的Put对象赋值
                byte[] row = result.getRow();
                inBoxPut.addColumn(Bytes.toBytes(Constant.INBOX_CF), Bytes.toBytes(attend), ts++ ,row);
            }
        }

        // 7 插入数据
        if (!inBoxPut.isEmpty()){
            Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
            inboxTable.put(inBoxPut);
            inboxTable.close();
        }

        //  释放资源
        contentTable.close();
        relationTable.close();
        connection.close();

    }


    /**
     * a、在微博用户关系表中，对当前主动操作的用户移除取关的好友(attends)
     * b、在微博用户关系表中，对被取关的用户移除粉丝
     * c、微博收件箱中删除取关的用户发布的微博
     * @param uid
     * @param attends
     */
    public static void removeAttends(String uid, String... attends) throws IOException {
        // 判断输入是否合理
        if (uid == null || uid.length() <= 0 || attends.length <= 0 || attends == null) {
            System.out.println("输入不合理");
            return;
        }
        // part1：操作关系表
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        // 1 获取表
        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));
        // 2 创建delete的集合
        ArrayList<Delete> deleteList = new ArrayList<>();
        // 2 关注者需要删除偶像，创建关注者的delete，添加到List
        Delete uDelete = new Delete(Bytes.toBytes(uid));
        for (String attend : attends) {
            uDelete.addColumn(Bytes.toBytes(Constant.RELATION_CF1), Bytes.toBytes(attend));
        }
        deleteList.add(uDelete);

        // 3 被关注者需要删除粉丝，创建被关注者的delete对象，添加属性，添加到List
        for (String attend : attends) {
            Delete delete = new Delete(Bytes.toBytes(attend));
            delete.addColumn(Bytes.toBytes(Constant.RELATION_CF2), Bytes.toBytes(uid));
            deleteList.add(delete);
        }
        // 4 删除
        relationTable.delete(deleteList);


        // part2: 操作收件箱表
        // 1 获取表的连接
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
        // 2 创建delete对象，添加属性
        Delete delete = new Delete(Bytes.toBytes(uid));
        for (String attend : attends) {
            delete.addColumn(Bytes.toBytes(Constant.INBOX_CF), Bytes.toBytes(attend));
        }
        // 3 执行删除
        inboxTable.delete(delete);

        // 释放资源
        inboxTable.close();
        relationTable.close();
        connection.close();
    }


    /**
     * 获取某个用户的初始化页面
     * @param uid
     */
    public static void getInit(String uid) throws IOException {
        // 获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        // 获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX_TABLE));
        // 获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
        // 创建收件箱表GET对象，并获取数据
        Get get = new Get(Bytes.toBytes(uid));
        Result result = inboxTable.get(get);
        // 遍历获取的数据
        for (Cell cell : result.rawCells()) {
            byte[] rowKey = CellUtil.cloneValue(cell);
            // 构建微博内容表GET对象
            Get cGet = new Get(rowKey);
            cGet.setMaxVersions();
            // 获取该GET对象的数据内容
            Result contentResult = contentTable.get(cGet);
            for (Cell rawCell : contentResult.rawCells()) {
                System.out.println("rowKey:"+Bytes.toString(CellUtil.cloneRow(rawCell))+"\t"
                +"columnFamily:"+Bytes.toString(CellUtil.cloneFamily(rawCell))+"\t"
                +"column:"+Bytes.toString(CellUtil.cloneQualifier(rawCell))+"\t"
                +"value:"+Bytes.toString(CellUtil.cloneValue(rawCell)));
            }

        }
        // 释放资源
        contentTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     *  通过过滤器，获取指定用户的所有微博内容
     * @param uid
     */
    public static void getWeibo(String uid) throws IOException {
        // 获取Connection连接对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONFIGURATION);
        // 获取内容表
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
        // 创建一个scan对象
        Scan scan = new Scan();
        // 设置scan的行键过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);
        // 获取数据
        ResultScanner results = contentTable.getScanner(scan);
        // 解析并输出
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+"\t"
                        +"columnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+"\t"
                        +"column:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"
                        +"value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

}
