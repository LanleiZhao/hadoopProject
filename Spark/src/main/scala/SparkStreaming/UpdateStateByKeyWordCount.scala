package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lucas
 * @create 2020-07-18-18:43
 */
object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    // 初始化StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("updatestateByKey").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("Error")
    // 设置检查点目录
    ssc.sparkContext.setCheckpointDir("./checkpoint")

    // 读取指定接口数据
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // wordcount
    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))

    val wordAndOneDStream: DStream[(String, Int)] = wordsDStream.map((_, 1))

    val wordCount: DStream[(String, Int)] = wordAndOneDStream.updateStateByKey((value: Seq[Int], state: Option[Int]) => {
      var temp: Int = state.getOrElse(0)
      for (elem <- value) {
        temp += elem
      }
      Option(temp)
    })

    // 打印结果
    wordCount.print()

    // 启动
    ssc.start()
    ssc.awaitTermination()

  }

}
