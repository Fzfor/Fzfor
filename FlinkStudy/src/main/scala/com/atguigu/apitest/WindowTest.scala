package com.atguigu.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConversions.iterableAsScalaIterable

/**
 *
 * .window以后得到 windowStream
 *
 * window操作，两个主要步骤
 * 1.窗口分配器 .window
 * 2.窗口函数，reduce aggregate apply process
 *
 * window类型
 * 通过窗口分配器来决定，时间窗口，计数窗口
 * 按照窗口起止时间（个数）的定义，可以有滚动窗口，滑动窗口，会话窗口
 *
 * 滑动窗口中，每条数据可以属于多个窗口，属于size/slide个窗口，
 *
 * 会话窗口：窗口长度不固定，需要指定的是间隔时间
 *
 * 窗口函数
 *    窗口函数是基于当前窗口内的数据的，是有界数据集的计算，通常只在窗口关闭时输出一次
 *    增量聚合函数：reduceFunction aggregateFunction，来一条聚合一次，得到中间状态，等待窗口关闭时输出，流式处理过程
 *    全窗口函数：windowFunction processWindowFunction 类似于批处理过程，所有数据来了以后把数据存下来，触发计算时再计算
 *
 * 乱序数据的影响
 *  当flink以event time模式处理数据流时，它会根据数据里的时间戳来处理基于时间的算子
 *
 *  由于网络、分布式等原因，会导致乱序数据的产生
 *  乱序数据会让窗口计算不正确
 *
 *  水位线
 *  怎样避免乱序数据带来计算不正确？
 *    遇到一个时间戳到了窗口关闭时间，不应该立刻触发窗口计算，而是等待一段时间，等迟到的数据来了再关闭窗口
 *
 *  watermark是一种衡量event time进展的机制，可以设定延迟触发
 *  watermark是用于处理乱序事件的，而正确的处理乱序事件，通常用watermark机制结合window实现
 *  数据流中watermark表示
 *
 *
 * 程序默认的时间语义，是processing time
 *
 *
 *
 * aggregate : 输出类型和reduce类型不一样
 * @author fzfor
 * @date 22:43 2021/05/18
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputStream = env.readTextFile("D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\main\\resources\\sensor.txt")

    val inputStream = env.socketTextStream("hadoop102", 7777)

    val dataStream = inputStream.map(data => {
      val datas = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })
//      .assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks()

    val result = dataStream
      .keyBy("id")
      //.window( EventTimeSessionWindows.withGap(Time.minutes(1)) )//会话窗口
      //      .timeWindow(Time.hours(1),Time.minutes(1))
      //      .window(TumblingEventTimeWindows.of(Time.hours(1),Time.hours(-8)))
      //      .countWindow(10,2)
      .timeWindow(Time.seconds(15))
      //      .reduce(new MyReduce)
      .apply(new MyWindowFunction())

    dataStream.print("data")
    env.execute("window test")
  }

}


//自定义一个全窗口函数
class MyWindowFunction() extends WindowFunction[SensorReading,(Long,Int),Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit =
    out.collect((window.getStart,input.size))
}

class MyReduce extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id,value1.timestamp.max(value2.timestamp),value1.temperature.min(value2.temperature))
  }
}