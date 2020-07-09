package Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author lucas
 * @create 2020-07-09-13:29
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {
    private int errorCount;
    private int successCount;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            errorCount++;
        } else{
            successCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("成功发送:"+successCount);
        System.out.println("发送失败:"+errorCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
