package chapter02

/**
 * 可变形参测试
 * @author lucas
 * @create 2020-07-14-15:47
 */
object VariableTest {
  def main(args: Array[String]): Unit = {
    val result = getSum(10, 1, 2, 3, 4, 5, 6)
    println("result = "+result)
  }

  def getSum(n:Int,args:Int*) ={
    var result = n
    for (elem <- args) {
      result += elem
    }
    result
  }
}
