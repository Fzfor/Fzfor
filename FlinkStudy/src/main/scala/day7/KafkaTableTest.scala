package day7

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @author fzfor
 * @date 11:16 2021/10/23
 */
object KafkaTableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    //连接kafka
    tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
        .property("zookeeper.connect","hadoop102:2181,hadoop103:2181,hadoop104:2181")
      )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    //做转换操作
    //对table进行转换操作，得到结果表
    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable.select('id, 'temperature)
      .filter('id === "sensor_1")

    val aggResultTable = sensorTable.groupBy('id)
      .select('id, 'id.count as 'count)

    //定义一个连接到kafka的输出表
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("test")
      .property("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
      .property("zookeeper.connect","hadoop102:2181,hadoop103:2181,hadoop104:2181")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        //.field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    //将结果表输出
    resultTable.insertInto("kafkaOutputTable")

    tableEnv.useCatalog()


    env.execute("kafka table test job")
  }

}
