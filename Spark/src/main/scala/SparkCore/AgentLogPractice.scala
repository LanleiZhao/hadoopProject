package SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计出每一个省份广告被点击次数的TOP3
 *
 * @author lucas
 * @create 2020-07-16-15:53
 */
object AgentLogPractice {
  def main(args: Array[String]): Unit = {
    // 1 创建SparkContext
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("agentLogCount")
    val sc = new SparkContext(conf)

    // 2 读取数据，创建RDD
    val textFileRDD: RDD[String] = sc.textFile("/Users/lucas/Study/BigData/data/agent.log")

    // 3 按照最小粒度聚合
    // 1516609143867 6 7 64 16
    // 时间戳，省份，城市，用户，广告
    val provinceAdtoOneRDD: RDD[((String, String), Int)] = textFileRDD.map {
      line =>
        val fields: Array[String] = line.split(" ")
        ((fields(1), fields(4)), 1)
    }

    // 4 计算每个省，每个广告的点击次数
    val provinceToAdCountRDD: RDD[((String, String), Int)] = provinceAdtoOneRDD.reduceByKey(_ + _)

    // 5 将省作为key,(ad,value)作为Value
    val provinceAdToCountRDD: RDD[(String, (String, Int))] = provinceToAdCountRDD.map(x => (x._1._1, (x._1._2, x._2)))

    // 6 将同一个省份的数据聚合
    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceAdToCountRDD.groupByKey()

    // 7 对同一个省份的集合进行排序，取前三
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues(x => x.toList.sortWith((x, y) => (x._2 > y._2)).take(3))

    // 8 收集到driver并打印
    provinceAdTop3.collect().foreach(println)

    // 9 关闭spark
    sc.stop()


  }
}
