package chapter01

/**
 * @author lucas
 * @create 2020-07-13-13:00
 */
object testScala {
  def main(args: Array[String]): Unit = {
    println("hello,world")
    val name = "lucas"
    val age = 20
    val city = "beijing"

    println(s"name=$name,age=$age,city=$city")
    printf("name=%s,age=%d,city=%s",name,age,city)
    
  }

}
