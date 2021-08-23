package fz.hbase.util.flink.outputFormat

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * 写入HBase提供两种方式
 * 第二种：实现OutputFormat接口
 *
 * @author fzfor
 * @date 9:48 2021/08/23
 */
class MyHBaseOutputFormat extends OutputFormat[String]{
  val zkServer = "192.168.1.103"
  val port = "2181"
  var conn: Connection = null
  var mutator: BufferedMutator = null
  var count = 0

  /**
   * 配置输出格式。此方法总是在实例化输出格式上首先调用的
   * @param parameters
   */
  override def configure(parameters: Configuration): Unit ={

  }

  /**
   * 用于打开输出格式的并行实例，所以在open方法中我们会进行hbase的连接，配置，建表等操作。
   * @param taskNumber
   * @param numTasks
   */
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    config.set(HConstants.ZOOKEEPER_QUORUM, zkServer)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)

    val tableName: TableName = TableName.valueOf("student")

    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    //设置缓存1m，当达到1m时数据会自动刷到hbase
    params.writeBufferSize(1024 * 1024) //设置缓存的大小
    mutator = conn.getBufferedMutator(params)
    count = 0
  }

  /**
   * 用于将数据写入数据源，所以我们会在这个方法中调用写入hbase的API
   * @param record
   */
  override def writeRecord(record: String): Unit = {
    val cf1 = "info"
    val array: Array[String] = record.split(",")
    val put: Put = new Put(Bytes.toBytes(array(0)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
    put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array(2)))
    mutator.mutate(put)
    //每4条刷新一下数据，如果是批处理调用outputFormat，这里填写的4必须不能大于批处理的记录总数量，否则数据不会更新到hbase里面
    if (count >= 2){
      mutator.flush()
      count = 0
    }
    count = count + 1
  }

  /**
   * 关闭
   */
  override def close(): Unit = {
    try {
      if (conn != null) conn.close()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}
