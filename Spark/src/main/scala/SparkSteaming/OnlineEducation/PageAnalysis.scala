package SparkSteaming.OnlineEducation

import java.lang
import java.util.Properties

import Utils.ParseIpAddress
import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.ConnectionPool

import scala.collection.mutable

/**
 * @author lucas
 * @create 2020-07-22-17:19
 */
object PageAnalysis {
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
    val IpAndOneDStream: DStream[(String, Int)] = kafkaDStream.transform { rdd =>
//      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter {
      // 验证数据的合法性，过滤状态码不为200的数据
      item => filterInvalidItem(item)
    }.map {
      // 将数据转换为(province,1)的格式
      item => convertToIPandOne(item)
    }

    // 窗口计算，每隔20秒计算前60秒的数据
    val reduceWinDStream: DStream[(String, Int)] = IpAndOneDStream.reduceByKeyAndWindow((a:Int,b:Int)=>a+b, Seconds(processingInterval * 4), Seconds(processingInterval * 2))

    // 业务逻辑筛选，一分钟内超过50次的省份
    val filteredDStream: DStream[(String, Int)] = reduceWinDStream.filter(item => filterInnormalIps(item))

    // 保存结果和offset到MySQL
    filteredDStream.foreachRDD{messages =>
      // 返回结果到Driver
      val offsetRanges: Array[OffsetRange] = messages.asInstanceOf[HasOffsetRanges].offsetRanges
      val resultTuple: Array[(String, Int)] = messages.collect()
      // 开启事务
      DB.localTx{ implicit session =>
        resultTuple.foreach(msg => {
          val province: String = msg._1
          val cnt: Int = msg._2
          // 统计结果写入MySQL
          println(msg)
          sql"replace into hot_province(province,cnt) values(${province},${cnt})".executeUpdate().apply()
        })

        // 保存偏移量
        for (o <- offsetRanges) {
          println(o.topic,o.partition,o.fromOffset,o.untilOffset)
          sql"update topic_offset set `offset`=${o.untilOffset} where group_id=${groupId} and topic=${o.topic} and `partition`=${o.partition}".executeUpdate().apply()
        }
      }

    }

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
    val ip: String = fields(1)
    val province: String = ParseIpAddress.getProvince(ip)
    (province,1)
  }

  def filterInnormalIps(item: (String, Int)):Boolean={
    // 获取province
    val province: String = item._1
    // 获取访问次数
    val cnt: Int = item._2
    // 当访问次数大于50次，去业务库查询是否存在
    if (cnt>50) {
      val result: List[String] = DB.readOnly { implicit session =>
        sql"select province from province where province=${province}"
          .map(_.get[String](1))
          .list().apply()
      }
      // 如果结果为空，说明不是热门省份
      if (result.isEmpty) {
        true
      } else{
        false
      }
    }
    false
  }

}
