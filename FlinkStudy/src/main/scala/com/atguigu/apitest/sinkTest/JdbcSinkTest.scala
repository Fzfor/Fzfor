package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

/**
 *
 *
 *
 * @author fzfor
 * @date 22:48 2021/05/10
 */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从文件中读取数据
    val inputDataStream = env.readTextFile("D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\main\\resources\\sensor.txt")
    val dataStream = inputDataStream.map(
      data => {
        val datas = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )

    dataStream.addSink(new MyJdbcSink())

    env.execute("jdbc sink test")

  }

}


//自定义一个sinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //首先定义sql连接，以及预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //在open生命周期方法中创建连接以及预编译语句
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    insertStmt = conn.prepareStatement("insert into temp (sensor,temperature) values (?,?)")
    updateStmt = conn.prepareStatement("update temp set temperature = ? where sensor = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    //如果刚才没有更新数据，那么执行插入操作
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  //关闭操作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}