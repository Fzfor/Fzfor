package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * redis工具类，用于获取Jedis连接，操作Redis
 *
 * @author fzfor
 * @date 12:32 2023/02/04
 */
object MyRedisUtils {

  var jedisPool: JedisPool = null

  def getJedisFromPool() = {
    if (jedisPool == null) {
      //创建连接池对象
      val host = MyPropsUtils(MyConfig.REDIS_HOST)
      val port = MyPropsUtils(MyConfig.REDIS_PORT)
      //连接池配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)

    }
    jedisPool.getResource

  }


}
