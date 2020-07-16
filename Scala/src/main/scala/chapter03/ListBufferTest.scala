package chapter03

import scala.collection.mutable.ListBuffer

/**
 * @author lucas
 * @create 2020-07-15-21:01
 */
object ListBufferTest {
  def main(args: Array[String]): Unit = {
    val lb: ListBuffer[Int] = ListBuffer[Int](1, 2, 3)
    lb.append(1,2,4)
    println(lb)
    lb.remove(0)
    println(lb)
  }

}
