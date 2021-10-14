package fz.redis.flink.sink

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
/**
 * @author fzfor
 * @date 16:34 2021/08/25
 */
class MyRedisSink extends RedisMapper[String] {
  // 定义redis操作
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"map2")
  }

  // 定义键的取值
  override def getKeyFromData(t: String): String = {
    t.split("-")(0)
  }

  // 定义值的取值
  override def getValueFromData(t: String): String = {
    t.split("-")(1)
  }
}
