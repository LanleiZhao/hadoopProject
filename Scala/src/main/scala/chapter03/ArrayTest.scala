package chapter03

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author lucas
 * @create 2020-07-15-18:58
 */
object ArrayTest {
  def main(args: Array[String]): Unit = {
    // 定长数组的第一种方式
    val a1 = new Array[Int](10)
    a1(0) = 1
    a1(1) = 4
    a1(2) = 6
//    for (elem <- a1) {
//      println(elem)
//    }

    // 定长数组的第二种方式
    val a2: Array[Int] = Array[Int](2, 3, 4, 5)
    for (elem <- a2.iterator) {
      println(elem)
    }

    // 变长数组
    val ab1: ArrayBuffer[String] = ArrayBuffer[String]("a", "b", "c")
    ab1.append("hello","scala")
    for (elem <- ab1) {
      println(elem)
    }
    println(ab1(0))  // 访问第一个元素

    // 定长数组和变长数组的转换
    val buffer: mutable.Buffer[Int] = a1.toBuffer
    val ab1Array: Array[String] = ab1.toArray
    println(ab1Array.length)
    println(buffer.length)



  }
}
