package fz.hbase.util.flink

import fz.hbase.util.flink.inputFormat.MyHBaseInputFormat
import fz.hbase.util.flink.outputFormat.MyHBaseOutputFormat
import fz.hbase.util.flink.sink.HBaseWriterSink
import fz.hbase.util.flink.source.HBaseReaderSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @author fzfor
 * @date 10:35 2021/08/19
 */
object Flink2HBase {
  def main(args: Array[String]): Unit = {
//    readFromHBaseWithRichSourceFunction()
//    readFromHBaseWithTableInputFormat()
//    println("---")
//    readFromHBaseSetWithTableInputFormat

//    write2HBaseWithRichSinkFunction
//    write2HBaseWithOutputFormat
    write2HBaseWithOutputFormat2
  }

  /**
   * 从HBase读取数据
   * 第一种：继承RichSourceFunction重写父类方法
   *
   * @return
   */
  def readFromHBaseWithRichSourceFunction() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val dataStream: DataStream[(String, String)] = env.addSource(new HBaseReaderSource)
    val value = dataStream.map(x => println(x._1 + " " + x._2))
      .print()
    env.execute()
  }

  /**
   * 从HBase读取数据
   * 第二种：实现TableInputFormat接口
   */
  def readFromHBaseWithTableInputFormat(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val dataStream = env.createInput(new MyHBaseInputFormat)
    dataStream
      //.filter(_.f0.startsWith("10"))
      .print()
    env.execute()
  }

  /**
   * 读取HBase数据方式：实现TableInputFormat接口
   */
  def readFromHBaseSetWithTableInputFormat(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.createInput(new MyHBaseInputFormat)
    dataStream
      .filter(_.f0.startsWith("20"))
      .print()
  }

  /**
   * 写入HBase
   * 第一种：继承RichSinkFunction重写父类方法
   */
  def write2HBaseWithRichSinkFunction(): Unit = {
    val topic = "test"
    val props = new Properties
    props.put("bootstrap.servers", "192.168.1.103:9092")
    props.setProperty("auto.offset.reset", "latest")
    props.put("group.id", "kv_flink")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    val dataStream: DataStream[String] = env.addSource(myConsumer)
    dataStream.print()
    //写入HBase
    dataStream.addSink(new HBaseWriterSink)
    env.execute()
  }

  /**
   * 写入HBase
   * 第二种：实现OutputFormat接口
   */
  def write2HBaseWithOutputFormat(): Unit = {
    val topic = "test"
    val props = new Properties
    props.put("bootstrap.servers", "192.168.1.102:9092")
    props.put("group.id", "kv_flink")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
    val dataStream: DataStream[String] = env.addSource(myConsumer)
    dataStream.writeUsingOutputFormat(new MyHBaseOutputFormat)
    env.execute()
  }

  /**
   * 写入HBase方式：实现OutputFormat接口
   */
  def write2HBaseWithOutputFormat2(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.定义数据
    val dataSet: DataSet[String] = env.fromElements("103,zhangsan,20", "104,lisi,21", "105,wangwu,22", "106,zhaolilu,23")
    dataSet.output(new MyHBaseOutputFormat)
    //运行下面这句话，程序才会真正执行，这句代码针对的是data sinks写入数据的
    env.execute()
  }

}
