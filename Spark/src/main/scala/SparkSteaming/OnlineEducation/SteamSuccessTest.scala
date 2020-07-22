package SparkSteaming.OnlineEducation

import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, _}

/**
 * @author lucas
 * @create 2020-07-22-17:19
 */
object SteamSuccessTest {
  // 从properties获取参数
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("PageAnalysis.properties"))
  // 获取jdbc相关参数
  val driver = prop.getProperty(Constant.JDBC_DRIVER)
  val jdbcUrl = prop.getProperty(Constant.JDBC_URL)
  val jdbcUser = prop.getProperty(Constant.JDBC_USER)
  val jdbcPassword = prop.getProperty(Constant.JDBC_PASSWORD)

  // 获取批处理的时间间隔
  val processingInterval = prop.getProperty(Constant.PROCESSING_INTEVAL).toLong

  // 获取kafka参数
  val brokers = prop.getProperty(Constant.BROKERS)
  val topic = prop.getProperty(Constant.TOPIC)
  val groupId = prop.getProperty(Constant.GROUPID)

  // 设置jdbc
//  Class.forName("com.mysql.jdbc.Driver")
  Class.forName(driver)
  // 设置连接池
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)


  def main(args: Array[String]): Unit = {
   // 初始化SparkConf
   val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
     .setMaster("local[*]")
     .set("spark.streaming.stopGracefullyOnShutdown", "true")
     .set("spark.serializer",classOf[KryoSerializer].getName)
    // 创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(processingInterval))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean))

    // 从Mysql获取offset
    val fromOffsets:Map[TopicPartition, Long] = DB.readOnly { implicit session =>
    sql"select topic,`partition`,`offset` from topic_offset where group_id=${groupId}".
      map { r =>
        new TopicPartition(r.string(1),r.int(2)) -> r.long(3)
      }.list.apply().toMap
   }

    val array: Array[String] = Array(topic)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      if (fromOffsets.size > 0) {
      // mysql中记录了该topic的分区偏移量，则从此处开始消费
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](array,kafkaParams, fromOffsets)
      )
    } else {
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](array, kafkaParams))
    }


    // 业务计算
    kafkaDStream.print()

    // 启动任务
    ssc.start()
    ssc.awaitTermination()



  }

  def filterInvalidItem(item: ConsumerRecord[String, String]): Boolean ={
    // 判断数据长度
    val lines: String = item.value()
    val fields: Array[String] = lines.split(" ")
    if (fields.length == 6){
      // 判断状态码，200的返回
      val statusCode: String = fields(5)
      if("200".equals(statusCode)){
        true
      } else{
        false
      }
    } else{
      false
    }
  }

  def convertToIPandOne(item: ConsumerRecord[String, String])={
    val fields: Array[String] = item.value().split(" ")
    (fields(1),1)
  }

  def filterInnormalIps(item: (String, Int)):Boolean={
    // 获取ip
    val uid: String = item._1
    // 获取访问次数
    val cnt: Int = item._2
    // 当访问次数大于3次，去业务库查询是否是VIP
    if (cnt!=3) {
      val result: List[String] = DB.readOnly { implicit session =>
        sql"select user_id from vip_users where user_id=${uid}"
          .map(_.get[String](1))
          .list().apply()
      }
      // 如果结果为空，说明不是Vip
      if (result.isEmpty) {
        true
      } else{
        false
      }
    }
    false
    // 返回不是VIP的用户ip
  }

}
