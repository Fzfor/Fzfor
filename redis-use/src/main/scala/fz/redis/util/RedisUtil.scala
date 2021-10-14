package fz.redis.util

import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool, JedisPoolConfig}

import java.util
/**
 * @author fzfor
 * @date 15:05 2021/08/25
 */
object RedisUtil {
  var jedisPool:JedisPool=null
  var jedis : Jedis = null

  def main(args: Array[String]): Unit = {
    val jedis_conn = getJedisClient
    jedis_conn.set("hello","world")
    closeJedisClient

    val conn = getJedis
    conn.set("geshou","ball")
    println(conn.get("hello"))
    closeJedis
  }

  def getJedis = {
    if (jedis == null) {
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")
      jedis = new Jedis(host, port.toInt)
    }
    jedis
  }

  def  closeJedis = {
    if (jedis != null) {
      println("--------关闭redis连接--------")
      jedis.close()
    }
  }

  /**
   * 单机 连接池的形式
   * @return
   */
  def getJedisClient:Jedis = {
    if(jedisPool == null){
      //      println("开辟一个连接池")
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(10)  //最大连接数
      jedisPoolConfig.setMaxIdle(4)   //最大空闲
      jedisPoolConfig.setMinIdle(4)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
    }

    jedisPool.getResource
  }

  def closeJedisClient = {
    print("--------关闭redis连接--------")
    if (jedisPool != null) {
      jedisPool.close()
    }
  }

  /**
   * 连接redis集群
   */
  def getJedisCluster ={
    //创建jedisCluster对象，有一个参数 nodes是Set类型，Set包含若干个HostAndPort对象
    val nodes = new util.HashSet[HostAndPort]()
    nodes.add(new HostAndPort("192.168.241.133",7001))
    nodes.add(new HostAndPort("192.168.241.133",7002))
    nodes.add(new HostAndPort("192.168.241.133",7003))
    nodes.add(new HostAndPort("192.168.241.133",7004))
    nodes.add(new HostAndPort("192.168.241.133",7005))
    nodes.add(new HostAndPort("192.168.241.133",7006))
    val jedisCluster = new JedisCluster(nodes)
    //使用jedisCluster操作redis
    jedisCluster.set("test", "my forst jedis")
    val str = jedisCluster.get("test")
    println(str)
    //关闭连接池
    jedisCluster.close()
  }
}
