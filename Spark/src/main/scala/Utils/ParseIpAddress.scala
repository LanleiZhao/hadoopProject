package Utils

import net.ipip.ipdb.{City, CityInfo}

/**
 * IP地址解析工具类
 * @author lucas
 * @create 2020-07-22-14:53
 */
object ParseIpAddress {
  val db = new City("/Users/lucas/Study/BigData/IDEAproject/hadoopProject/Spark/src/main/resources/ipipfree.ipdb")

  /**
   * 获取IP地址对应的省份
   * @param ip
   * @param lang
   * @return
   */
  def getCountry(ip:String,lang:String="CN"):String={
    val info: Array[String] = db.find(ip, lang)
    info(0)
  }

  /**
   * 获取IP地址对应的省份
   * @param ip
   * @param lang
   * @return
   */
  def getProvince(ip:String,lang:String="CN"):String={
    val info: Array[String] = db.find(ip, lang)
    info(1)
  }

  /**
   * 获取IP地址对应的城市
   * @param ip
   * @param lang
   * @return
   */
  def getCity(ip:String, lang:String="CN"):String={
    val info: Array[String] = db.find(ip, lang)
    info(2)
  }

}
