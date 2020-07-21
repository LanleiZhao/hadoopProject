package SparkSteaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 实现从网络接口中实时读取数据，并用spark-sql统计单词个数
 * 因此要用到Spark-Streaming,Spark-Sql
 *
 * @author lucas
 * @create 2020-07-21-10:27
 */
object SqlNetworkWordCountPractice {
  def main(args: Array[String]): Unit = {
    // 参数校验
    if(args.length != 2){
      System.err.println("\"Usage: SqlNetworkWordCountPractice <hostname> <port>")
      System.exit(1)
    }
    // 初始化配置信息
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    // 初始化Spark-Streaming
    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.sparkContext.setLogLevel("Warn")
    // 创建socketDStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))
    // 在DStream内部初始化SparkSession
    wordDStream.foreachRDD{ (rdd,time) =>
      val spark: SparkSession = SparkSessionSingle.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      // 将DStream的每个RDD转换DataFrame
      val df: DataFrame = rdd.toDF("word")
      // 注册为临时表
      df.createOrReplaceTempView("word_table")
      // 执行Spark-sql统计
      println(time)
      spark.sql("select word,count(*) as cnt from word_table group by word").show()
    }

    // 启动Streaming
    ssc.start()
    // 等待程序退出
    ssc.awaitTermination()
  }

}

object SparkSessionSingle{
  private var instance:SparkSession = _

  def getInstance(conf:SparkConf):SparkSession={
    if(instance == null){
      instance = SparkSession.builder().config(conf).getOrCreate()
    }
    instance
  }
}

