package weibo.com.lucas.dao;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-10-15:18
 */
public class HBaseDaoTest {
    public static void main(String[] args) throws IOException {
        HBaseDao.publishWeibo("12","hello");
    }
}
