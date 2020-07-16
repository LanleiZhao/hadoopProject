package chapter03

/**
 * @author lucas
 * @create 2020-07-15-15:29
 */
class Person(xname:String, xage:Int){
  var name = xname
  var age = xage
  var gender = 'M'

  def this(yname:String,yage:Int,ygender:Char){
    this(yname,yage)
    this.gender = ygender
  }

  def say(message:String): Unit ={
    println(message)
  }


}

object PersonTest {
  def main(args: Array[String]): Unit = {
    val p = new Person("lucas", 20)
    p.name = "jack"
    p.age = 24
    println(p.name+"\t"+p.age)

    val p2 = new Person("jack", 21, 'M')
    p2.say("hello, scala")

  }
}
