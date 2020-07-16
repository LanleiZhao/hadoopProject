package chapter03

/**
 * @author lucas
 * @create 2020-07-15-19:12
 */
object ListTest {
  def main(args: Array[String]): Unit = {
    val li: List[Int] = List(1, 2, 5)
//    for (elem <- li) {
//      println(elem)
//    }

    val li2: List[Int] = li :+ (100)
    println(li)
    println(li2)

    val li3: List[Int] = li.+:(200)
    println(li3)

    val li4 = List(1,2,3,"abc")
    val li5 = 4::5::6::Nil
    println(li4)
    println(li5)
    val li6 = 4::5::6::li4::Nil
    println(li6)
    val li66 = 4::5::6::li4
    println(li66)

  }
}
