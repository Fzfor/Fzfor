package fz.redis.flink

import fz.redis.flink.sink.MyRedisSink
import fz.redis.flink.source.MyRedisSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

/**
 * @author fzfor
 * @date 16:26 2021/08/25
 */
object Flink2Redis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //调用addSource以此来作为数据输入端
    val stream = env.addSource(new MyRedisSource)

    //redis连接参数设置
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.1.102")
      .setPort(6379)
      .build()

    // 调用addSink以此来作为数据输出端
    stream.addSink(new RedisSink[String](conf, new MyRedisSink))

    // 打印流
    stream.print()

    // 执行主程序
    env.execute()
  }
}
