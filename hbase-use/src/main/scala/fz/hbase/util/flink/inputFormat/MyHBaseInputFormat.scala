package fz.hbase.util.flink.inputFormat


import org.apache.hadoop.hbase.client.{Connection, Result, Scan}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import java.io.IOException
import scala.jdk.CollectionConverters.asScalaBufferConverter


/**
 *
 * 从HBase读取数据
 * 第二种：实现TableInputFormat接口
 *
 * @author fzfor
 * @date 11:56 2021/08/19
 */
class MyHBaseInputFormat extends TableInputFormat[Tuple2[String,String]]{

  // 结果Tuple
  val tuple2 = new Tuple2[String,String]


  /**
   * 建立HBase连接
   * @param parameters
   */
  override def configure(parameters: Configuration): Unit = {
    val tableName: TableName = TableName.valueOf("student")
    val cf1 = "info"
    var conn: Connection = null
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create

    config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.1.102")
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    try {
      conn = ConnectionFactory.createConnection(config)
      table = conn.getTable(tableName).asInstanceOf[HTable]
      scan = new Scan()
//      scan.setStartRow(Bytes.toBytes("001"))
//      scan.setStopRow(Bytes.toBytes("201"))
      scan.addFamily(Bytes.toBytes(cf1))
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  override def getScanner: Scan ={
    scan
  }

  override def getTableName: String = "student"

  override def mapResultToTuple(result: Result): Tuple2[String, String] = {
    val rowKey = Bytes.toString(result.getRow)
    val sb = new StringBuffer()
    for (cell: Cell <- result.listCells().asScala){
      val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      sb.append(value).append("_")
    }
    val value = sb.replace(sb.length() - 1, sb.length(), "").toString
    tuple2.setField(rowKey, 0)
    tuple2.setField(value, 1)
    tuple2
  }
}
