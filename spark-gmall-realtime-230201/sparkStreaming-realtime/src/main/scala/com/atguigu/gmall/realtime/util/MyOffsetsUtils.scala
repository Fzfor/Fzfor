package com.atguigu.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.util
import scala.collection.mutable

/**
 * offset管理工具类，用于往redis中存储和读取offset
 *
 * 管理方法：
 * 1.后置提交偏移量 -> 手动控制偏移量提交
 * 2.手动控制偏移量提交 -> SparkStreaming提供了手动提交方案，但是我们不能用，因为我们会对DStream的结构进行转换。
 * 3.手动的提取偏移量维护到redis中
 * -> 从kafka中消费到数据，先提取偏移量
 * -> 邓数据成功写出后，将偏移量存储到redis中
 * -> 从kafka中消费数据之前，先到redis中读取偏移量，使用读取到的偏移量到kafka中消费数据
 *
 * @author fzfor
 * @date 13:16 2023/02/04
 */
object MyOffsetsUtils {

  /**
   * 在redis中怎么存？
   * 类型：string，list，set，zset，hash
   * key：groupid + topic
   * value：partition + offset
   * 写入API：hset / hmset
   * 读取API：hgetall
   * 是否过期：不过期
   *
   */


  /**
   * 往redis中存储offset
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
    if (offsetRanges != null && offsetRanges.length > 0) {
      val offsets = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition = offsetRange.partition
        val endOffset = offsetRange.untilOffset
        offsets.put(partition.toString, endOffset.toString)
      }

      //存入redis
      val jedis = MyRedisUtils.getJedisFromPool()
      val redisKey = s"offset:${topic}:${groupId}"
      jedis.hset(redisKey, offsets)
      println("提交的offsets：" + offsets)
      jedis.close()

    }
  }


  /**
   * 从redis中读取存储的offset
   */
  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis = MyRedisUtils.getJedisFromPool()
    val results = mutable.Map[TopicPartition, Long]()
    val redisKey = s"offset:${topic}:${groupId}"
    val offsets = jedis.hgetAll(redisKey)
    println("读取到的offsets" + offsets)
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsets.asScala) {
      val topicPartition = new TopicPartition(topic, partition.toInt)
      results.put(topicPartition, offset.toLong)
    }
    jedis.close()
    results.toMap
  }

}
