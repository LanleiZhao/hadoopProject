package Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 时间戳拦截器
 * @author lucas
 * @create 2020-07-09-13:22
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord producerRecord) {
        // 创建一个新的record，把时间戳写入消息体的最前部
        ProducerRecord<String, String> record = new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(),
                (String) producerRecord.key(), System.currentTimeMillis() + "," + producerRecord.value().toString());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
