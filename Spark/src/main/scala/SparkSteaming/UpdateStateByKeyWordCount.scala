package SparkSteaming

import breeze.linalg.sum
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lucas
 * @create 2020-07-20-21:45
 */
object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    // 初始化配置
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 设置日志级别
    ssc.sparkContext.setLogLevel("Warn")
    // 设置检查点目录，保存状态
    ssc.sparkContext.setCheckpointDir("hdfs://master:9000/checkpoint")
    // 创建SocketDStream
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // z整理数据
    val wordAndOneDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_, 1))

    // 定义updateState函数
    def updateFunction(values: Seq[Int], prev: Option[Int]): Option[Int] = {
      val sum: Int = values.sum
      val pr: Int = prev.getOrElse(0)
      Option(sum + pr)
    }

    // 有状态计算
//    val updateStateDStream: DStream[(String, Int)] = wordAndOneDStream.updateStateByKey(updateFunction)

      val updateStateDStream: DStream[(String, Int)] = wordAndOneDStream.updateStateByKey((values: Seq[Int], prev: Option[Int]) => (Option(sum(values) + prev.getOrElse(0))))
    // 打印结果
    updateStateDStream.print()
    // 执行
    ssc.start()
    ssc.awaitTermination()

  }

}
