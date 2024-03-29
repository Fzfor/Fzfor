package com.atguigu.gmall.realtime.util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
 * Kafka工具类， 用于生产数据和消费数据
 *
 * @author fzfor
 * @date 14:41 2023/02/01
 */
object MyKafkaUtils {
  /**
   * 消费者配置
   *
   * ConsumerConfig
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // kafka集群位置
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils("kafka.bootstrap-servers"),
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),

    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // groupId
    // offset提交  自动 手动
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //自动提交的时间间隔
    //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    // offset重置  "latest"  "earliest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    // .....
  )

  /**
   * 基于SparkStreaming消费，获取到kafkaDStream， 使用默认的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaSDtream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaSDtream
  }

  /**
   * 基于SparkStreaming消费，获取到kafkaDStream， 使用指定的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaSDtream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets))
    kafkaSDtream
  }


  /**
   * 生产者对象
   */
  val producer: KafkaProducer[String, String] = createProducer()


  /**
   * 创建生产者对象
   */
  def createProducer() = {
    val producerConfigs = new util.HashMap[String, AnyRef]
    //生产者配置类： ProducerConfig
    //kafka集群配置
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092" )
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    //kv序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    //batch.size 16kb
    //lingeer.ms 0
    //幂等配置
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val producer = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /**
   * 生产（按照默认的粘性分区策略）
   *
   */
  def send(topic: String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 生产（按照key进行分区）
   */
  def send(topic: String, key: String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * 关闭生产者
   */
  def close() = {
    if (producer != null) producer.close()
  }

  /**
   * 刷写，将缓冲区的数据刷写到磁盘
   */
  def flush(): Unit = {
    producer.flush()
  }

}
