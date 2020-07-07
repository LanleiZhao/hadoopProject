package com.lucas.mapreduce.UdfInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-07-00:27
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    private Configuration configuration;
    private FileSplit split;
    private boolean isProgress = true;
    private BytesWritable value = new BytesWritable();
    private Text k = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
            // 定义缓存区
            byte[] contents = new byte[(int) split.getLength()];
            // 获取文件系统
            FileSystem fs = FileSystem.get(configuration);
            // 读取数据
            Path path = split.getPath();
            FSDataInputStream fis = fs.open(path);
            // 读取文件内容
            IOUtils.readFully(fis,contents,0,contents.length);
            // 输出文件内容
            value.set(contents,0,contents.length);
            // 获取文件路径和名称
            String name = split.getPath().toString();
            // 设置输出的key
            k.set(name);
            IOUtils.closeStream(fis);
            isProgress  = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
