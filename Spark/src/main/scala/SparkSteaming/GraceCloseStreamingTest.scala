package SparkSteaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lucas
 * @create 2020-07-20-18:37
 */
object GraceCloseStreamingTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }
    // 1 初始化配置
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    // 2 创建SparkStreaming
    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.sparkContext.setLogLevel("WARN")
    // 3 创建fileStrem
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream(args(0), args(1).toInt)
    // 4 逻辑处理
    val wordDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wordDStream.print()

    // 5 启动
    ssc.start()
//    ssc.awaitTermination()
    val hdfs_file = "hdfs://master:9000/stop"
    stopByMarkFile(ssc,hdfs_file)

  }

  /**
   * 根据hdfs文件判断是否关闭程序
   * @param ssc
   * @param hadoop_file
   */
  def stopByMarkFile(ssc:StreamingContext, hadoop_file:String)={
    // 定义扫描文件是否存在的间隔
    val intervalMills = 2* 1000 // 2秒
    var isStop = false
    while (!isStop){
      isStop = ssc.awaitTerminationOrTimeout(intervalMills) // 是否已经停止
      if(!isStop && isExistsMarkFile(hadoop_file)){
        println("1秒后开始关闭Streaming程序...")
        Thread.sleep(1000)
        ssc.stop(true,true)
        deleteHdfsFile(hadoop_file)
      } else{
        println("没有检测有停止信号")
      }
    }
  }

  /**
   * 判断文件是否存在
   * @param hdfs_file_path
   * @return
   */
  def isExistsMarkFile(hdfs_file_path: String):Boolean={
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs: FileSystem = path.getFileSystem(conf)
    fs.exists(path)
  }

  /**
   * 删除hdfs指定文件路径
   * @param hdfs_file_path
   * @return
   */
  def deleteHdfsFile(hdfs_file_path: String)={
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs = path.getFileSystem(conf)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
  }

}
