package com.lucas.mapreduce.FlowMR;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-06-22:05
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 取出一行
        String line = value.toString();

        // 分割为数组
        String[] fields = line.split("\t");

        // 取出手机号作为key
        String phone = fields[1];

        // 封装FlowBean作为value，取出upFlow,downFlow
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        // 设置key
        Text k = new Text(phone);

        // 设置value
        FlowBean v = new FlowBean(upFlow, downFlow);

        // 写出
        context.write(k,v);
    }
}
