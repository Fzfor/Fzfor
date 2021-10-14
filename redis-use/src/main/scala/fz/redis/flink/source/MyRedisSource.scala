package fz.redis.flink.source

import fz.redis.util.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.Jedis

/**
 * @author fzfor
 * @date 16:08 2021/08/25
 */
class MyRedisSource extends RichSourceFunction[String]{
  var jedis_conn:Jedis = null


  override def open(parameters: Configuration): Unit = {
    jedis_conn = RedisUtil.getJedisClient
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val mapKeys = jedis_conn.hkeys("map1")
    val iter = mapKeys.iterator()
    while (iter.hasNext) {
      val key = iter.next()
      val value = jedis_conn.hget("map1", key)
      ctx.collect(key+"-"+value)
    }
  }

  override def cancel(): Unit = {
    println("close client")
    jedis_conn.close()
  }
}
