package day5

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * @author fzfor
 * @date 22:02 2021/06/17
 */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.socketTextStream("hadoop102", 7777)

    val dataStream = inputStream.map(data => {
      val datas = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    //用ProcessFunction的测输出流实现分流操作
    val highTempStream = dataStream.process(new SplitTempProcessor(30) )

    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String,Double,Long)]("low-temp"))

    //打印输出
    highTempStream.print("high")
    lowTempStream.print("low")

    env.execute("side output job")


  }
}

//自定义ProcessFunction，用于区分高低温度的数据
class SplitTempProcessor(threshold: Int) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    //判断当前数据的温度值，如果大于阈值，输出到主流，如果小于阈值，输出到测输出流
    if (value.temperature > threshold) {
      out.collect(value)
    }else{
      ctx.output(new OutputTag[(String,Double,Long)]("low-temp"),(value.id,value.temperature,value.timestamp))
    }
  }
}