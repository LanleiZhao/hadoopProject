package com.lucas.mr.MapJoin;

import com.luas.mr.MapJoin.DistributedCacheMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 将商品信息表中数据根据商品pid合并到订单数据表中,使用MapJoin。
 * @author lucas
 * @create 2020-07-07-12:19
 */
public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        // 参数配置
        args = new String[]{"src/main/java/com/lucas/mapreduce/MapJoin/order.txt",
                "src/main/java/com/lucas/mapreduce/MapJoin/output"};

        // 创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar
        job.setJarByClass(DistributedCacheDriver.class);
        // 设置map类
        job.setMapperClass(DistributedCacheMapper.class);
        // 设置map输出key类型
        job.setOutputKeyClass(Text.class);
        // 设置value输出value类型
        job.setOutputValueClass(NullWritable.class);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置分布式缓存路径
        job.addCacheFile(new URI("src/main/java/com/lucas/mapreduce/MapJoin/product.txt"));
        job.setNumReduceTasks(0);
        // 提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
