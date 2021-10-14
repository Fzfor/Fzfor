package day5

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util
import java.util.concurrent.TimeUnit

/**
 * @author fzfor
 * @date 22:59 2021/06/17
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //checkpoint相关配置
    //启用检查点，指定触发检查点间隔时间
    env.enableCheckpointing(1000L)
    //其他配置
    val ckConfig = env.getCheckpointConfig
    ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    ckConfig.setCheckpointTimeout(30000L)
    ckConfig.setMaxConcurrentCheckpoints(1)
    ckConfig.setMinPauseBetweenCheckpoints(50000L)
    ckConfig.setPreferCheckpointForRecovery(true)
    ckConfig.setTolerableCheckpointFailureNumber(100)

    //重启策略的配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,10000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)))


    val inputStream = env.socketTextStream("hadoop102", 7777)

    val dataStream = inputStream.map(data => {
      val datas = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    val warningStream = dataStream.keyBy(_.id)
      //.flatMap(new TempChangeWarningWithFlatmap(10.0))
      //R:输出的类型 S:状态的类型
      .flatMapWithState[(String,Double,Double),Double]({
        case (inputData:SensorReading,None) => (List.empty,Some(inputData.temperature))
        case (inputData:SensorReading,lastTemp: Some[Double]) => {
          val diff = (inputData.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((inputData.id,lastTemp.get,inputData.temperature)),Some(inputData.temperature))
          }else{
            (List.empty,Some(inputData.temperature))
          }
        }
      })

    warningStream.print()

    env.execute("state test job")
  }

}

//自定义RichMapFunction
class TempChangeWarning(threshold : Double) extends RichMapFunction[SensorReading,(String,Double,Double)] {
  //定义状态变量 上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))
  }

  override def map(value: SensorReading): (String, Double, Double) = {
    //从状态中取出上一次的温度值
    val lastTemp = lastTempState.value()
    //更新状态
    lastTempState.update(value.temperature)
    //跟当前温度值计算差值 然后和阈值比较 如果大于就报警
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      (value.id,lastTemp,value.temperature)
    }else {
      (value.id,0,0)
    }
  }
}

//自定义richFlatMapFunction
class TempChangeWarningWithFlatmap(threshold: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)] {
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))
  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //从状态中取出上一次的温度值
    val lastTemp = lastTempState.value()
    //更新状态
    lastTempState.update(value.temperature)
    //跟当前温度值计算差值 然后和阈值比较 如果大于就报警
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id,lastTemp,value.temperature))
    }
  }
}

class MyProcessor extends KeyedProcessFunction[String,SensorReading,Int]{
  //没有lazy的话会报错，因为刚运行的时候没有运行时上下文
  //lazy val myState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state",classOf[Int]))

  lazy val myListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("my-listState",classOf[String]))
  lazy val myMapState: MapState[String,Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Double]("my-mapState",classOf[String],classOf[Double]))
  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading](
    "my-reducingState",
    new ReduceFunction[SensorReading] {
      override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = SensorReading(value1.id,value1.timestamp.max(value2.timestamp),value1.temperature.min(value2.temperature))
    },
    classOf[SensorReading]))

  //另一种方式
  var myState: ValueState[Int] = _
  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state",classOf[Int]))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
    myState.value()
    myState.update(1)

    myListState.add("hello")
    myListState.addAll(new util.ArrayList[String]())
    myListState.update(new util.ArrayList[String]())

    myMapState.put("sensor_1",10.0)
    myMapState.get("sensor_2")
    myMapState.contains("hello")

    myReducingState.add(value)
    myReducingState.get()
    myReducingState.clear()
  }
}