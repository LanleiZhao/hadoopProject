package com.lucas.mr.CommonFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-07-16:45
 */
public class TwoCommonFriendReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        for (Text friend : values) {
            stringBuilder.append(friend).append(" ");
        }
        context.write(key, new Text(stringBuilder.toString()));
    }
}
