package ScalikeJDBC

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
 * @author lucas
 * @create 2020-07-22-12:12
 */
object ScalikeTest1 {
  def main(args: Array[String]): Unit = {
    // 加载配置
    DBs.setup()

    // select
    val a: List[Int] = DB.readOnly(implicit session => {
      SQL("select * from emp").map(rs => {
        rs.int(0)
      }).list().apply()
    })

    // insert
    DB.localTx(implicit session =>{
      SQL("insert into tablename values(?,?,?)").bind("","","").update().apply()
    })

    // update
    DB.autoCommit(implicit session=>{
      SQL("update tablename set columname=? where columname=?").bind("","").update().apply()
    })

    // delete
    DB.autoCommit(implicit session =>{
      SQL("delete from database where xx=?").bind("").update().apply()
    })

  }

}
