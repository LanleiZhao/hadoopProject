package chapter02

/**
 * @author lucas
 * @create 2020-07-14-10:58
 */
object ForTest {
  def main(args: Array[String]): Unit = {
    for(i<- 1 to 10;j = i+1 if i%2==0) {
      println("i = "+i+",j = " +j)
    }

    for(i <- 4 until(10)) {
      println(i)
    }

    val list = List("hello", "abc", 123)
    for (elem <- list) {
      println(elem)
    }

    val range = 0.until(5)
    println(range.toList)


  }

}
