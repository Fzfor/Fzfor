package day4

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * @author fzfor
 * @date 23:24 2021/06/10
 */
object ProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.socketTextStream("hadoop102", 7777)

    val dataStream = inputStream.map(data => {
      val datas = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })


    //检测每一个传感器温度是否连续上升，在10秒内
    val warningStream = dataStream.keyBy("id")
      .process(new TempIncreWarning(10000L))

    warningStream.print()

    env.execute("process function job")
  }

}

//自定义keyedProcessFunction
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple,SensorReading,String] {
  //由于需要跟之前的温度值做对比，所以将上一个温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  //为了方便删除定时器，还需要保存定时器的时间戳
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer-ts",classOf[Long]))

  //定时器触发，说明10秒内没有来下降的温度值，报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度值连续" + interval /1000 +"秒上升")
    curTimerTsState.clear()

  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //首先取出状态
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerTsState.value()

    //将上次温度值的状态更新为当前数据的温度值
    lastTempState.update(value.temperature)

    //判断当前温度值，如果比之前温度高，并且没有定时器，注册10秒后的定时器
    if (value.temperature > lastTemp && curTimerTs == 0) {
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerTsState.update(ts)
    }
    //如果温度下降，删除定时器
    else if (value.temperature < lastTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      //清空状态
      curTimerTsState.clear()
    }
  }
}