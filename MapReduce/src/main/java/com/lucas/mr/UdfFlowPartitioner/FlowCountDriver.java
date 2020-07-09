package com.lucas.mr.UdfFlowPartitioner;

import com.luas.mr.UdfFlowPartitioner.FlowCountMapper;
import com.luas.mr.UdfFlowPartitioner.FlowCountReducer;
import com.luas.mr.UdfFlowPartitioner.ProvincePartitioner;
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
public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置参数，文件逻辑
        args = new String[]{"src/main/java/com/lucas/mapreduce/FlowUdfPartitioner/phone_data.txt",
                "src/main/java/com/lucas/mapreduce/FlowUdfPartitioner/flowCountOutput"};
        // 创建job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar所在的路径
        job.setJarByClass(FlowCountDriver.class);
        // 设置map类
        job.setMapperClass(FlowCountMapper.class);
        // 设置reduce类
        job.setReducerClass(FlowCountReducer.class);
        // 设置map的key输出类型
        job.setMapOutputKeyClass(Text.class);
        // 设置map的value输出类型
        job.setMapOutputValueClass(FlowBean.class);
        // 设置reduce的key输出类型
        job.setOutputKeyClass(Text.class);
        // 设置reduce的value输出类型
        job.setMapOutputValueClass(FlowBean.class);

        // 设置自定义分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        // 设置reducetask的个数
        job.setNumReduceTasks(5);

        // 设置文件输入目录，输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交应用程序，等待结束
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);


    }
}
