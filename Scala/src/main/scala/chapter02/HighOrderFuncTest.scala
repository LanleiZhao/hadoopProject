package chapter02

/**
 * @author lucas
 * @create 2020-07-14-15:56
 */
object HighOrderFuncTest {
  // 函数的第一个参数类型是另外一个函数
  def apply(f:Int=>String,v:Int)=f(v)

  def fmtInt(n:Int):String={
    "整数值:"+n
  }

  def main(args: Array[String]): Unit = {
    val str = apply(fmtInt, 100)
    println(str)
  }

}
