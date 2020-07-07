package com.lucas.mapreduce.WebLogETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author lucas
 * @create 2020-07-07-14:00
 */
public class LogDriver {
        public static void main(String[] args) throws Exception {

            // 参数配置
            args = new String[]{"src/main/resources/web.log",
                    "src/main/java/com/lucas/mapreduce/WebLogETL/output"};
            // 1 获取job信息
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            // 2 加载jar包
            job.setJarByClass(LogDriver.class);

            // 3 关联map
            job.setMapperClass(LogMapper.class);

            // 4 设置最终输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // 5 设置输入和输出路径
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 6 提交
            job.waitForCompletion(true);
        }

}
