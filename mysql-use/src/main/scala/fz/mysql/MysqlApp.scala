package fz.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author fzfor
 * @date 9:57 2021/09/03
 */
object MysqlApp {
  //mysql连接
  var connection: Connection = _
  var preparedStatement: PreparedStatement = _
  val host = "192.168.1.102"
  val port = 3306
  val db = "asiaJob"
  val user = "root"
  val password = "000000"

  def main(args: Array[String]): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&useSSL=false&allowMultiQueries=true",
      user,
      password)
    val sql =
      """
        |select app_code, task_code,task_cells, task_labels, task_start_time, task_end_time,task_state
        |from s1mme_task_info
        |""".stripMargin

    preparedStatement = connection.prepareStatement(sql)
    val resultSet = preparedStatement.executeQuery()
    while (resultSet.next()) {
      val app_code = resultSet.getString("app_code")
      val task_code = resultSet.getInt("task_code")
      val task_cells = resultSet.getString("task_cells")
      val task_labels = resultSet.getString("task_labels")
      val task_state = resultSet.getInt("task_state")
      println(app_code + "|" + task_code + "|" + task_cells + "|" + task_labels+ "|" +task_state)
      println(app_code.length + "|" + task_code + "|" + task_cells.length + "|" + task_labels.length+ "|" +task_state.hashCode())
    }

    close()
  }

  /**
   * 执行完以后，关闭连接，释放资源
   */
  def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (preparedStatement != null) {
      preparedStatement.close()
    }
  }

}
