package day7

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * 输出表计算的结果到文件
 * @author fzfor
 * @date 21:18 2021/10/21
 */
object OutputTableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream.map(data => {
      val datas = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    //创建表环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    //将datastream转换成table
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)

    //对table进行转换操作得到结果表
    val resultTable = sensorTable.select('id, 'temp)
      .filter('id === "sensor_1")

    val aggResultTable = sensorTable.groupBy('id)
      .select('id, 'id.count as 'count)

    //定义一张输出表，这就是要写入数据的TableSink
    tableEnv.connect(new FileSystem().path("D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\main\\resources\\out.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("cnt",DataTypes.DOUBLE())
        //.field("ts",DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")

    //将结果表写入 tablesink
    resultTable.insertInto("outputTable")

    //sensorTable.printSchema()
    //sensorTable.toAppendStream[(String,Double,Long)].print()


    env.execute("output test")
  }
}
