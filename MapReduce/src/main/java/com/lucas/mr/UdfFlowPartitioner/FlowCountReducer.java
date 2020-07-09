package com.lucas.mr.UdfFlowPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lucas
 * @create 2020-07-06-22:17
 */
public class FlowCountReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 遍历values,upFlow和downFlow累加
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean bean : values) {
            sumUpFlow += bean.getUpFlow();
            sumDownFlow += bean.getDownFlow();
        }
        // 封装为flowBean
        FlowBean resultFlowBean = new FlowBean(sumUpFlow, sumDownFlow);

        // 写出key,value
        context.write(key, resultFlowBean);
    }
}
