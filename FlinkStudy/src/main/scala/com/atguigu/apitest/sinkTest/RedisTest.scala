package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author fzfor
 * @date 7:47 2021/05/11
 */
object RedisTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从文件中读取数据
    val inputDataStream = env.readTextFile("D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\main\\resources\\sensor.txt")
    val dataStream = inputDataStream.map(
      data => {
        val datas = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )

    //定义一个配置redis的配置类
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()

    //定义一个redisMapper，连接以后，需要用redis的命令来存储数据
    val myMapper = new RedisMapper[SensorReading] {
      //定义保存数据到redis的命令，hset table_name key value
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
      }

      override def getKeyFromData(t: SensorReading): String = t.id

      override def getValueFromData(t: SensorReading): String = t.temperature.toString
    }

    dataStream.addSink(new RedisSink[SensorReading](conf,myMapper))

    env.execute("redis sink test")

  }

}
