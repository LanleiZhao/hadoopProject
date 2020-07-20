package SparkStreaming

/**
 * @author lucas
 * @create 2020-07-20-01:53
 */
package com.spark

import java.sql.{DriverManager, ResultSet}

import Constant.Constants
import io.netty.handler.codec.string.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object MysqlOffset {


  def main(args: Array[String]): Unit = {


    //1.创建StreamingContext
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    //准备连接Kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkKafkaTest",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Set("carevents")

    val offsetMap: mutable.Map[TopicPartition, Long] = getOffsetMap(kafkaParams("group.id").toString, topics.toString())

    val recordDStream: DStream[ConsumerRecord[String, String]] = if (offsetMap.size > 0) { //有记录offset
      println("MySQL中记录了offset,则从该offset处开始消费")


      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetMap)) //消费策略,源码强烈推荐使用该策略
    } else { //没有记录offset
      println("没有记录offset,则直接连接,从latest开始消费")
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)) //消费策略,源码强烈推荐使用该策略
    }

    recordDStream.foreachRDD {
      messages =>
        if (messages.count() > 0) { //当前这一时间批次有数据
          messages.foreachPartition { messageIter =>
            messageIter.foreach { message =>
              println(message.toString())
            }
          }
          val offsetRanges: Array[OffsetRange] = messages.asInstanceOf[HasOffsetRanges].offsetRanges
          for (o <- offsetRanges) {
            println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},untilOffset=${o.untilOffset}")
          }
          //手动提交offset,默认提交到Checkpoint中
          //recordDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
          //实际中偏移量可以提交到MySQL/Redis中
          saveOffsetRanges("SparkKafkaTest", offsetRanges)
        }
    }
    ssc.start()
    ssc.awaitTermination()
  }


  //  从数据库读取偏移量
  def getOffsetMap(groupid: String, topic: String) = {
    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection(Constants.mysqlURL,Constants.mysqlUser,Constants.mysqlPassword)
    val sqlselect = connection.prepareStatement("""
      select * from kafka_offset
      where groupid=? and topic =?
     """)
    sqlselect.setString(1, groupid)
    sqlselect.setString(2, topic)
    val rs: ResultSet = sqlselect.executeQuery()
    val offsetMap = mutable.Map[TopicPartition, Long]()
    while (rs.next()) {
      offsetMap += new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("offset")
    }
    rs.close()
    sqlselect.close()
    connection.close()
    offsetMap
  }


  //  将偏移量保存到数据库
  def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]) ={
    val connection = DriverManager.getConnection(Constants.mysqlURL, Constants.mysqlUser,Constants.mysqlPassword)
    //replace into表示之前有就替换,没有就插入
    val select_ps = connection.prepareStatement("""
  select count(*) as count from kafka_offset
  where  `groupid`=? and `topic`=? and `partition`=?
  """)
    val update_ps = connection.prepareStatement("""
  update kafka_offset set  `offset`=?
  where `groupid`=? and `topic`=? and `partition`=?
  """)
    val insert_ps = connection.prepareStatement("""
  INSERT INTO kafka_offset(`groupid`, `topic`, `partition`, `offset`)
  VALUE(?,?,?,?)
  """)
    for (o <- offsetRange) {
      select_ps.setString(1, groupid)
      select_ps.setString(2, o.topic)
      select_ps.setInt(3, o.partition)
      val select_resut = select_ps.executeQuery()
      // println(select_resut.)// .getInt("count"))
      while (select_resut.next()) {
        println(select_resut.getInt("count"))
        if (select_resut.getInt("count") > 0) {
          //update
          update_ps.setLong(1,o.untilOffset)
          update_ps.setString(2, groupid)
          update_ps.setString(3, o.topic)
          update_ps.setInt(4, o.partition)
          update_ps.executeUpdate()
        } else {
          //insert
          insert_ps.setString(1, groupid)
          insert_ps.setString(2, o.topic)
          insert_ps.setInt(3, o.partition)
          insert_ps.setLong(4, o.untilOffset)
          insert_ps.executeUpdate()
        }
      }


    }
    select_ps.close()
    update_ps.close()
    insert_ps.close()
    connection.close()
  }


}