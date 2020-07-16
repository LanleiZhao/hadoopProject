package chapter03

import scala.io.StdIn

/**
 * @author lucas
 * @create 2020-07-15-16:18
 */
object LoopTest {
  def ifTest() {
    // if else
    println("input your age:")
    val age = StdIn.readInt()
    if (age < 20) {
      println(s"your age is $age,less than 20")
    } else if (age >= 20 && age < 30) {
      println(s"your age is $age,less than 30 and more than 20")
    } else {
      println(s"your age is $age,more than 30")
    }
  }

  def ninenineTable(): Unit = {
    for (i <- 1 to 9) {
      for (j <- 1 to i) {
        print(s"$i*$j=${i * j}\t")
      }
      println()
    }
  }

  def forTest(): Unit = {
    for (i <- 1 to 50 if i % 2 == 0 if i % 3 == 0) {
      println(i)
    }

  }

  def main(args: Array[String]): Unit = {
    //        ifTest()
    //      ninenineTable()
    forTest()

  }


}
