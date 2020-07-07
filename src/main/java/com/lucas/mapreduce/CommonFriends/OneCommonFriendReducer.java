package com.lucas.mapreduce.CommonFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-07-16:45
 */
public class OneCommonFriendReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder stringBuilder = new StringBuilder();
        for (Text person : values) {
            stringBuilder.append(person).append(",");
        }
        context.write(key, new Text(stringBuilder.toString()));
    }

}
