package fz.hbase.util.flink.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._

import scala.jdk.CollectionConverters.asScalaBufferConverter

/**
 *
 * 以HBase为数据源
 * 从HBase中获取数据，然后以流的形式发射
 *
 * 从HBase读取数据
 * 第一种：继承RichSourceFunction重写父类方法
 *
 * @author fzfor
 * @date 10:55 2021/08/19
 */
class HBaseReaderSource extends RichSourceFunction[(String, String)] {
  private var conn: Connection = null
  private var table: Table = null
  private var scan: Scan = null

  /**
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()

    config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.1.102")
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    val tableName: TableName = TableName.valueOf("student")
    val cf1: String = "info"
    conn = ConnectionFactory.createConnection(config)
    table = conn.getTable(tableName)
    scan = new Scan()
    //    scan.setStartRow(Bytes.toBytes("100"))
    //    scan.setStopRow(Bytes.toBytes("107"))
    scan.addFamily(Bytes.toBytes(cf1))
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
    val rs = table.getScanner(scan)
    val iterator = rs.iterator()
    while (iterator.hasNext) {
      val result = iterator.next()
      val rowKey = Bytes.toString(result.getRow)
      val sb: StringBuffer = new StringBuffer()
      for (cell: Cell <- result.listCells().asScala) {
        //val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        sb.append(value).append("_")
      }
      val valueString = sb.replace(sb.length() - 1, sb.length(), "").toString
      ctx.collect((rowKey, valueString))
    }
  }

  override def cancel(): Unit = ???

  override def close(): Unit = {
    try {
      if (table != null) {
        table.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e:Exception => println(e.getMessage)
    }
  }
}
