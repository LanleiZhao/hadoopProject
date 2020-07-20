package SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lucas
 * @create 2020-07-16-15:20
 */
object CombineByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("combinebykey")
    val sc = new SparkContext(conf)

    val array: Array[(String, Int)] = Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    // 创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(array)
    // 计算每种key对应的均值
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (a1: (Int, Int), a2: (Int, Int)) => (a1._1 + a2._1, a1._2 + a2._2))
    val resultRDD: RDD[(String, Double)] = combineRDD.map { case (k, v) => (k, v._1 / v._2.toDouble) }

    val result: Array[(String, Double)] = resultRDD.collect()
    for (elem <- result) {
      println(elem)
    }

    // 释放资源
    sc.stop()


  }

}
