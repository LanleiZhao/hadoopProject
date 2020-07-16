package chapter03

/**
 * @author lucas
 * @create 2020-07-15-18:41
 */
object TupleTest {
  def main(args: Array[String]): Unit = {
    val t1 = (1, "one")
    val t2 = (2, "two")

    val t3 = 3 -> "three"
    val t4 = 4 -> "four" -> "四"

    println(t1._1+"\t"+t1._2)
    val t11 = t1.productElement(0)
    val t12 = t1.productElement(1)
    println(t11+"\t"+t12)

    println(t4._1._2)

    val t5 =(1,"bcd","hello",true)
    for (elem <- t5.productIterator) {
      println(elem)
    }

    val t6: ((Int, String), Boolean) = ((1, "one"), true)

    
  }

}
