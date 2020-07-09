package com.lucas.mr.MapJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lucas
 * @create 2020-07-07-12:19
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        FileInputStream fileInputStream = new FileInputStream(path);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        // 分割
        String line;
        while ((line=bufferedReader.readLine()) != null){
            String[] fields = line.split("\t");
            // 缓存数据到map
            pdMap.put(fields[0],fields[1]);
        }
        // 关流
       bufferedReader.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text k = new Text();
        // 取出每一行
        String line = value.toString();
        // 分割
        String[] fields = line.split("\t");
        // 获取pid
        String pid = fields[1];
        // 从pdMap中获取pName
        String pName = pdMap.get(pid);
        // 拼接
        k.set(line+"\t"+pName);
        // 写出
        context.write(k, NullWritable.get());
    }

}
