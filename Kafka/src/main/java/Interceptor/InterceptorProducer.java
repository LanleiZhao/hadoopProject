package Interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author lucas
 * @create 2020-07-09-13:32
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        // 1 设置配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        // 2 构建拦截链
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("Interceptor.TimeInterceptor");
        interceptors.add("Interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        // 3 构建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 4 发送消息
        String topic = "first";
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topic, "message" + i));
        }
        // 5 关闭producer
        producer.close();
    }
}
