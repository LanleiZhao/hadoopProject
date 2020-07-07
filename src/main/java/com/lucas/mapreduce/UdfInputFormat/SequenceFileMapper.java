package com.lucas.mapreduce.UdfInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-07-00:50
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text,BytesWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }

}
