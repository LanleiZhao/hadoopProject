package Utils

/**
 * @author lucas
 * @create 2020-07-22-15:10
 */
object IpParseTest {
  def main(args: Array[String]): Unit = {
    while (true) {
      val ip: String = MockRandomData.getRandomIp
      println(ip+"\t"+ParseIpAddress.getProvince(ip)+"\t"+ParseIpAddress.getCity(ip))
    }
  }

}
