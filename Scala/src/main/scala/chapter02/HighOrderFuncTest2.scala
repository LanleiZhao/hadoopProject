package chapter02

/**
 * @author lucas
 * @create 2020-07-14-16:03
 */
object HighOrderFuncTest2 {
  // 函数的返回值是一个函数
  def addBy(n:Int)={
    (d:Double) => n+d
  }

  def test1(x:Double)=(y:Double)=>x*x*y



  def main(args: Array[String]): Unit = {
    println(addBy(10)(2.3))
    println(test1(2.0)(3.0))

  }


}
