package weibo.dao;

import org.junit.Test;
import weibo.constants.Constant;
import weibo.utils.HBaseUtil;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-10-15:18
 */
public class HBaseDaoTest {

    @Test
    public void testInit() throws IOException {
        // 创建命名空间
//        HBaseUtil.createNameSpace("weibo");
        // 先删除删除
        HBaseUtil.deleteTable(Constant.CONTENT_TABLE);
        HBaseUtil.deleteTable(Constant.RELATION_TABLE);
        HBaseUtil.deleteTable(Constant.INBOX_TABLE);
        // 创建内容表
        HBaseUtil.createTable(Constant.CONTENT_TABLE, Constant.CONTENT_TABLE_VERSION, Constant.CONTENT_TABLE_CF);
        // 创建关系表
        HBaseUtil.createTable(Constant.RELATION_TABLE,Constant.RELATION_TABLE_VERSION,Constant.RELATION_CF1,Constant.RELATION_CF2);
        // 创建收件箱表
        HBaseUtil.createTable(Constant.INBOX_TABLE,Constant.INBOX_TABLE_VERSION,Constant.INBOX_CF);
    }

    @Test
    public void testInitData() throws IOException {
        // 101发布微博
        HBaseDao.publishWeibo("101", "今天是个好天气");
        // 102发布微博
        HBaseDao.publishWeibo("102", "今天周末，嗨起来");
        // 102发布微博
        HBaseDao.publishWeibo("102", "玩了两局王者农药，连跪，呜呜～");
        // 103关注101，102
        HBaseDao.addAttends("103", "101", "102");
        // 104关注102
        HBaseDao.addAttends("104", "102");
    }


    @Test
    public void testGetData() throws IOException {
        // 获取103的初始微博
        HBaseDao.getInit("103");
        System.out.println("-------------------------");
        // 获取102发的全部微博
        HBaseDao.getWeibo("102");
    }
}
