package SparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lucas
 * @create 2020-07-16-00:40
 */
object WordCount {
  def main(args: Array[String]): Unit = {
//    val input = "/Users/lucas/Study/BigData/data/source.txt"
//    val output = "/Users/lucas/Study/BigData/data/sparkWCouptut"
    // 创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("wordCount")
    // 创建SparkContext
    val sc = new SparkContext(conf)
    // 创建RDD并执行WordCount
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .saveAsTextFile(args(1))
    // 释放资源
    sc.stop()
  }

}
