package SparkSteaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lucas
 * @create 2020-07-20-18:37
 */
object SteamingTest {
  def main(args: Array[String]): Unit = {
    // 1 初始化配置
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    // 2 创建SparkStreaming
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setLogLevel("WARN")
    // 3 创建fileStrem
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    // 4 逻辑处理
    val wordDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wordDStream.print()

    // 5 启动
    ssc.start()
    ssc.awaitTermination()
  }

}
