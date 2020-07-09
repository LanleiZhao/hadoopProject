package com.lucas.mr.UdfInputFormat;

import com.luas.mr.UdfInputFormat.SequenceFileMapper;
import com.luas.mr.UdfInputFormat.SequenceFileReducer;
import com.luas.mr.UdfInputFormat.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-07-00:53
 */
public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置路径
        args = new String[]{"/Users/lucas/Study/BigData/data/multifile","/Users/lucas/Study/BigData/data/multifile_output"};

        // 创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置JAR包
        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);

        // 设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 设置map,kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}
