package day7

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.{StreamTableEnvironment, table2RowDataSet}
import org.apache.flink.table.api.scala._

/**
 * @author fzfor
 * @date 22:14 2021/10/14
 */
object TableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream.map(data => {
      val datas = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //基于数据流，转换成一张表，然后进行操作
    val table = tableEnv.fromDataStream(dataStream)

    //调用table api 得到转换结果
    val resultTable = table.select("id,temperature")
      .filter("id == 'sensor_1'")

    //或者直接写sql得到转换结果
    val resultSqlTable = tableEnv.sqlQuery(
      s"""
         |select id,temperature from ${table} where id = 'sensor_1'
         |""".stripMargin)


    //转换成数据流，打印输出
    val value = resultSqlTable.toAppendStream[(String, Double)]
    value.print("--")
    resultTable.printSchema()

    env.execute("table example job")
  }

}
