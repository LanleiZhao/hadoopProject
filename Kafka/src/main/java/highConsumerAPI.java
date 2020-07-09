import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * kafka consumer高级API
 * @author lucas
 * @create 2020-07-09-10:42
 */
public class highConsumerAPI {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master:9092");
        // 制定consumer group
        props.put("group.id", "test");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的反序列化类
        props.put("key.deserializer", StringDeserializer.class);
        // value的反序列化类
        props.put("value.deserializer", StringDeserializer.class);
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费者订阅的topic
        List<String> topics = Arrays.asList("first");
        consumer.subscribe(topics);
        // 读取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("offset="+record.offset()+",key="+record.key()+",value="+record.value());
            }
        }
    }
}
