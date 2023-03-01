package day7

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}
import org.apache.flink.table.types.DataType

/**
 * 测试Table API
 * @author fzfor
 * @date 12:23 2021/05/18
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)

    //1.创建表环境
    //1.1 创建老版本的流查询环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    //1.2 创建老版本的批式查询环境
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(batchEnv)

    //1.3 创建blink版本的流查询环境
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)

    //1.4 创建blink版本的批式查询环境
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

    val bbTableEnv = TableEnvironment.create(bbSettings)

    //2. 从外部系统读取数据，在环境中注册表
    //2.1 连接到文件系统 csv
    val filePath = "D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
//      .withFormat(new OldCsv()) //定义读取数据之后的格式化方法 启用的oldcsv
      .withFormat(new Csv()) //定义读取数据之后的格式化方法 启用的oldcsv
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ) //定义表结构
      .createTemporaryTable("inputTable") //注册一张表

    //2.2连接kafka
    tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
        .property("zookeeper.connect","hadoop102:2181,hadoop103:2181,hadoop104:2181")
      )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    //3.表的查询
    //3.1 简单查询，过滤投影
    val sensorTable = tableEnv.from("inputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    //3.2 sql简单查询
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin)

    //resultSqlTable.toAppendStream[(String,Double)].print()

    //3.3简单聚合，统计每个传感器温度个数
    val aggResultTable = sensorTable.groupBy("id")
      .select('id, 'id.count as 'count)

    //3.4 sql实现简单聚合
    val aggResultSqlTable = tableEnv.sqlQuery("select id , count(id) as cnt from inputTable group by id")

    //resultTable.toAppendStream[(String,Double)].print("result")
    aggResultSqlTable.toRetractStream[(String,Long)].print("agg")


    //转换成流打印输出
//    val sensorTable = tableEnv.from("kafkaInputTable")
//    val value = sensorTable.toAppendStream[(String, Long, Double)].print()


    env.execute("table api test job")

  }

}
