package com.lucas.mr.CommonFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author lucas
 * @create 2020-07-07-16:45
 */
public class TwoCommonFriendMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] firend_persons = line.split("\t");

        String friend = firend_persons[0];
        String[] persons = firend_persons[1].split(",");

        Arrays.sort(persons);

        for (int i = 0; i < persons.length; i++) {
            for (int j = i+1; j < persons.length; j++) {
                Text k = new Text(persons[i] + "-" + persons[j]);
                Text v = new Text(friend);
                context.write(k, v);
            }
        }
    }
}
