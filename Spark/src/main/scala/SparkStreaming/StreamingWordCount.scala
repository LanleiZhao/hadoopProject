package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lucas
 * @create 2020-07-17-11:23
 */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    // 1 初始化配置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streamingWC")
    // 2 初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    // 3 创建DStream
    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9999, StorageLevel.MEMORY_ONLY)
    // 4 切分单词
    val wordStream: DStream[String] = lineStreams.flatMap(_.split(" "))
    // 5 映射为元组
    val wordAndOneStream: DStream[(String, Int)] = wordStream.map((_, 1))
    // 6 单词统计
    val wordAndCountStream: DStream[(String, Int)] = wordAndOneStream.reduceByKey(_ + _)
    // 7 打印
    wordAndCountStream.print()
    // 8 启动
    ssc.start()
    ssc.awaitTermination()
  }

}
