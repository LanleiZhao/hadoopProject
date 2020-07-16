package chapter03

import scala.collection.mutable

/**
 * @author lucas
 * @create 2020-07-15-21:44
 */
object MapTest {
  def main(args: Array[String]): Unit = {
//    val map: Map[Int, String] = Map[Int, String]()
//    map+(1 -> "hadoop")
//    map+(2 -> "scala")
//    map+(3 -> "spark")
//    for (elem <- map) {
//      println(elem._1+"\t"+elem._2)
//    }
    val map: Map[Int, String] = Map[Int, String](1 -> "hello", 2 -> "scala", 3 -> "spark")
    println(map)
    for (elem <- map.keys) {
      println(map.get(elem).get)
    }

    val map2: Map[Int, String] = map + (4 -> "flink")
    println(map)
    println(map2)

    val map3: mutable.Map[Int, String] = mutable.Map((1, "a"), (2, "b"), (3, "c"))
    map3 +=((4,"d"))
    println(map3)

    val map5: mutable.Map[Int, String] = mutable.Map[Int, String]()
    map5 += ((1,"hello"))
    map5 += (2 -> "world")
    println("map5 =   "+map5)
    map5 -=(1)
    println(map5)

  }

}
