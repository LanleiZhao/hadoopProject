package chapter03

/**
 * @author lucas
 * @create 2020-07-15-21:32
 */
import scala.collection.mutable
object QueneTest {
  def main(args: Array[String]): Unit = {
    val queue = new mutable.Queue[Int]
    queue += 20
    println(queue)
    queue ++= List(2,3,4)
    println(queue)
    val queue1: mutable.Queue[Int] = queue.+:(100)

    println(queue)
    println(queue1)
//    queue += List(5,6,7)
//    println(queue)

  }
}
