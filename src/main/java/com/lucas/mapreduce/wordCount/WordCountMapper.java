package com.lucas.mapreduce.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-02-00:48
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        // 分割
        String[] words = line.split(" ");
        // 输出
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }
}
