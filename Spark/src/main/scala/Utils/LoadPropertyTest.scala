package Utils

import java.io.FileInputStream
import java.util.Properties

/**
 * @author lucas
 * @create 2020-07-22-16:27
 */
object LoadPropertyTest {
  def main(args: Array[String]): Unit = {
    val pro = new Properties()
    pro.load(this.getClass.getClassLoader.getResourceAsStream("application.conf"))
    val str: String = pro.getProperty("db.default.url")
    println(str)
  }

}
