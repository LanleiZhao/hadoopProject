package chapter02

/**
 * @author lucas
 * @create 2020-07-14-14:40
 */
object YangHuiTest {
  def main(args: Array[String]): Unit = {
    // 在一层FOR循环打印杨辉三角
    for (i <- 1 to 18 if i % 2 != 0) {
      println(" " * ((18 - i) / 2) + "^" * i + " " * ((18 - i) / 2))
    }
    for (i <- 1 to 5) {
      println(" " * ((18 - 2) / 2) + "|" * 2)
    }
  }

}
