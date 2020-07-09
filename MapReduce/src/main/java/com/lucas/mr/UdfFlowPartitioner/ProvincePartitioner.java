package com.lucas.mr.UdfFlowPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lucas
 * @create 2020-07-07-10:18
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // 获取手机号前3位
        String preNum = text.toString().substring(0, 3);
        int partitioner = 4;
        // 判断哪个省
        if ("136".equals(preNum)) {
            partitioner = 0;
        } else if ("137".equals(preNum)) {
            partitioner = 1;
        } else if ("138".equals(preNum)) {
            partitioner = 2;
        } else if ("139".equals(preNum)) {
            partitioner = 3;
        }
        return partitioner;
    }
}
