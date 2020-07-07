package com.lucas.mapreduce.CommonFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author lucas
 * @create 2020-07-07-16:44
 */
public class OneCommonFriendMapper extends Mapper<LongWritable, Text,Text,Text> {
//    A:B,C,D,F,E,O


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取行
        String line = value.toString();
        // 切割，取出person,friends[]
        String person = line.split(":")[0];
        String[] friends = line.split(":")[1].split(",");
        // 写出<>
        for (String friend : friends) {
            context.write(new Text(friend),new Text(person));
        }

    }
}
