package Utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.ArrayBuffer

/**
 * @author lucas
 * @create 2020-07-22-13:54
 */
object MockRealTimeData {

  def createKafkaProducer(broker: String)={
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])

    // 根据配置创建Kafka生产者
    new KafkaProducer[String,String](prop)
  }

  def main(args: Array[String]): Unit = {
    // 获取配置信息
    val broker = "master:9092"
    val topic = "user_info"

    // 创建Kafka消费者
    val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer(broker)

    // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
    while (true){
      val records: Array[String] = generateMockDate()
      for (record <- records) {
        kafkaProducer.send(new ProducerRecord[String,String](topic,record))
      }
      Thread.sleep(5000)
    }

  }
  
  def generateMockDate()={
    val count:Int = 100
    // 定义arrayBuffer保存数据
    val arrayBuffer: ArrayBuffer[String] = ArrayBuffer[String]()

    for (i<-1 to count){
      val time: Long = System.currentTimeMillis()
      val ip: String = MockRandomData.getRandomIp
      val name: String = MockRandomData.getChineseName
      val tel: String = MockRandomData.getTel
      val email: String = MockRandomData.getEmail(5, 5)
      val statusCode: Int = MockRandomData.getStatusCode

      // 拼接数据
      val builder = new StringBuilder
      builder.append(time+"\t"+ip+"\t"+name+"\t"+tel+"\t"+email+"\t"+statusCode)
      val str: String = builder.toString()
      println(str)
      arrayBuffer.+=(str)
    }
    arrayBuffer.toArray
  }

}
