package com.lucas.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author lucas
 * @create 2020-06-27-22:33
 */
public class HdfsClient {
    // 创建目录测试
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");

        // 2 创建目录
        fs.mkdirs(new Path("/hdfstest"));

        // 3 关闭资源
        fs.close();
    }

    // 上传文件测试
    @Test
    public void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "root");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("/Users/lucas/Study/BigData/java/30days/src/day23/ForTest.java"),
                new Path("/hdfstest"));

        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }


}






