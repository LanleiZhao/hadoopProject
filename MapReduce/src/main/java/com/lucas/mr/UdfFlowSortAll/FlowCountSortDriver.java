package com.lucas.mr.UdfFlowSortAll;

import com.luas.mr.UdfFlowSortAll.FlowCountSortMapper;
import com.luas.mr.UdfFlowSortAll.FlowCountSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-06-22:23
 */
public class FlowCountSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置参数，文件逻辑
        args = new String[]{"src/main/java/com/lucas/mapreduce/UdfFlowSortAll/phone_data.txt",
                "src/main/java/com/lucas/mapreduce/UdfFlowSortAll/sortAllOutput"};
        // 创建job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar所在的路径
        job.setJarByClass(FlowCountSortDriver.class);
        // 设置map类
        job.setMapperClass(FlowCountSortMapper.class);
        // 设置reduce类
        job.setReducerClass(FlowCountSortReducer.class);
        // 设置map的key输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        // 设置map的value输出类型
        job.setMapOutputValueClass(Text.class);
        // 设置reduce的key输出类型
        job.setOutputKeyClass(Text.class);
        // 设置reduce的value输出类型
        job.setMapOutputValueClass(FlowBean.class);

        // 设置文件输入目录，输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交应用程序，等待结束
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);


    }
}
