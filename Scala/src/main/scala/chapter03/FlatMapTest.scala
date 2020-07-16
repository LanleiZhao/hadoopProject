package chapter03

/**
 * @author lucas
 * @create 2020-07-15-23:48
 */
object FlatMapTest {
  def main(args: Array[String]): Unit = {
    val names = List("Lucas","Jack","Tom")
    val chars: List[Char] = names.flatMap(_.toLowerCase)
    val list: List[String] = names.map(_.toLowerCase)
    val filters: List[String] = names.filter(_.startsWith("J"))
    println(chars)
    println(list)
    println(filters)
  }
}
