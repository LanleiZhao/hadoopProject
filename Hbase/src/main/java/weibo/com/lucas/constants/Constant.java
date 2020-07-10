package weibo.com.lucas.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


/**
 * @author lucas
 * @create 2020-07-10-14:33
 */
public class Constant {

    // 配置信息
    public static final Configuration CONFIGURATION = HBaseConfiguration.create();
    // 命名空间
    public static String NAMESPACE = "weibo";
    // 内容表
    public static String CONTENT_TABLE = "weibo:content";
    public static String CONTENT_TABLE_CF = "info";
    public static int CONTENT_TABLE_VERSION = 1;

    // 关系表
    public static String RELATION_TABLE = "weibo:relation";
    public static String RELATION_CF1 = "attends";
    public static String RELATION_CF2 = "fans";
    public static int RELATION_TABLE_VERSION = 1;

    // 收件箱表
    public static String INBOX_TABLE = "inbox";
    public static String INBOX_CF = "info";
    public static int INBOX_TABLE_VERSION = 2;

}
