package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author lucas
 * @create 2020-07-22-01:38
 */
object HiveTableTest {
  def main(args: Array[String]): Unit = {
    // 初始化SparkSession
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 导入隐式转换
    import spark.sql

    // 读取Hive
    sql("use hive")
    sql("show tables").show()
    sql("select * from hive.books").show()

    // 关闭Spark
    spark.stop()
  }
}
