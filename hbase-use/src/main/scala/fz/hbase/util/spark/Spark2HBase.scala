package fz.hbase.util.spark

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.util.Base64
import scala.collection.mutable.ListBuffer

/**
 * @author fzfor
 * @date 11:02 2021/08/18
 */
object Spark2HBase {
  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    writeToHBase("student")
        writeToHBaseNewAPI("student")
    //    readFromHBaseWithHBaseNewAPIScan("student")

    //windows用户没有权限写hdfs 需要配置
    //System.setProperty("HADOOP_USER_NAME", "atguigu")
    //insertWithBulkLoadWithMulti("student")

  }

  /**
   * 使用旧版本saveAsHadoopDataset保存数据到HBase上。
   *
   * @param tableNameStr 表名
   */
  def writeToHBase(tableNameStr: String) = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //获取sparkContext
    //spark 2.0以前的写法
    /*
    val conf = new SparkConf().setAppName("Spark2HBase").setMaster("local[4]")
    val sc = new SparkContext(conf)
    */
    val sparkSession = SparkSession.builder().appName("Spark2HBase").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    //创建HBase配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.1.102") //设置zookeeper集群，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181") //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableNameStr)


    //初始化job，设置输出格式，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val dataRDD = sc.makeRDD(Array("12,jack,16", "11,Lucy,15", "15,mike,17", "13,Lily,14"))

    val data = dataRDD.map { item =>
      val Array(key, name, age) = item.split(",")
      val rowKey = key.reverse
      val put = new Put(Bytes.toBytes(rowKey))
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据 须用 org.apache.hadoop.hbase.util.Bytes.toBytes 转换
       * Put.addColumn 方法接收三个参数：列族，列名，数据*/
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age))
      (new ImmutableBytesWritable(), put)
    }
    //保存到HBase表
    data.saveAsHadoopDataset(jobConf)
    sc.stop()
  }


  /**
   * 使用新版本saveAsNewAPIHadoopDataset保存数据到HBase上
   * saveAsNewAPIHadoopDataset
   */
  def writeToHBaseNewAPI(tableNameStr: String): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.1.102")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tableNameStr)

    val jobConf = new JobConf(hbaseConf)
    //设置job的输出格式
    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])

    val input = sc.textFile("D:\\Study\\IDEAProjects\\Fzfor\\hbase-use\\src\\main\\resources\\data.txt")

    val data = input.map { item =>
      val Array(key, name, age) = item.split(",")
      val rowKey = key.reverse
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age))
      (new ImmutableBytesWritable, put)
    }
    //保存到HBase表
    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sparkSession.stop()
  }

  /**
   * 使用newAPIHadoopRDD从hbase中读取数据，可以通过scan过滤数据
   *
   * @param tableNameStr 表名
   */
  def readFromHBaseWithHBaseNewAPIScan(tableNameStr: String): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession.builder().appName("SparkToHBase").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.1.102")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableNameStr)

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("info"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray))
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)

    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val dataRDD = hbaseRDD
      .map(x => x._2)
      .map { result =>
        //        (result.getRow, result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")), result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
        (result.getRow, result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
      }
      //      .map(row => (new String(row._1), new String(row._2), new String(row._3)))
      .map(row => (new String(row._1), new String(row._2)))
      .collect()
      //      .foreach(r => (println("rowKey:"+r._1 + ", name:" + r._2 + ", age:" + r._3)))
      .foreach(r => (println("rowKey:" + r._1 + ", name:" + r._2)))
  }

  /**
   * 批量插入 多列
   *
   * @param tableNameStr 表名
   */
  def insertWithBulkLoadWithMulti(tableNameStr: String): Unit = {

    val sparkSession = SparkSession.builder().appName("insertWithBulkLoad").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.1.103")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableNameStr)

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf(tableNameStr))

    val job = Job.getInstance(hbaseConf)
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableNameStr)))

    val rdd = sc.textFile("D:\\Study\\IDEAProjects\\Fzfor\\hbase-use\\src\\main\\resources\\data.txt")
      .map(_.split(","))
      .map(x => (DigestUtils.md5Hex(x(0)).substring(0, 3) + x(0), x(1), x(2)))
      .sortBy(_._1)
      .flatMap(x => {
        val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
        val kv1: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(x._2 + ""))
        val kv2: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(x._3 + ""))
        listBuffer.append((new ImmutableBytesWritable, kv2))
        listBuffer.append((new ImmutableBytesWritable, kv1))
        listBuffer
      }
      )
    //多列的排序，要按照列名字母表大小来
    isFileExist("hdfs://hadoop102:8020/test", sc)

    rdd.saveAsNewAPIHadoopFile("hdfs://hadoop102:8020/test", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("hdfs://hadoop102:8020/test"), admin, table, conn.getRegionLocator(TableName.valueOf(tableNameStr)))

    //关闭资源
    table.close()
    admin.close()
    conn.close()
    sc.stop()

  }

  /**
   * 判断hdfs上文件是否存在，存在则删除
   */
  def isFileExist(filePath: String, sc: SparkContext): Unit = {
    val output = new Path(filePath)
    val hdfs = FileSystem.get(new URI(filePath), new Configuration)
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }
  }

}
