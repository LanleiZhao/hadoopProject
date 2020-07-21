package SparkSteaming

import breeze.linalg.sum
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lucas
 * @create 2020-07-20-21:45
 */
object ReduceByKeyAndWindowTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }
    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    val ssc = new StreamingContext(conf, Seconds(5))
    // 设置日志级别
//    ssc.sparkContext.setLogLevel("Warn")
    // 设置检查点目录，保存状态
    ssc.sparkContext.setCheckpointDir("hdfs://master:9000/checkpoint")
    // 创建SocketDStream
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream(args(0), args(1).toInt)
    // 整理数据
    val wordAndOneDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_, 1))

    // 优化后的reduceByKeyAndWindow
    val reduceDStream: DStream[(String, Int)] = wordAndOneDStream.reduceByKeyAndWindow(_ + _, _ - _, Seconds(15), Seconds(10))

    // 打印结果
    reduceDStream.print()
    // 输出到fs

    // 执行
    ssc.start()
    ssc.awaitTermination()

  }

}
