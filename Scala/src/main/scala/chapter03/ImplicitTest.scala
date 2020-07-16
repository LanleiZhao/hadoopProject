package chapter03

/**
 * @author lucas
 * @create 2020-07-15-18:18
 */
object ImplicitTest {
  def main(args: Array[String]): Unit = {
    implicit def f(d:Double): Int ={
      d.toInt
    }

    implicit def f1(l:Long):Int={
      l.toInt
    }

    val d:Int = 10.5
    val l:Int = 1000L


  }

}
