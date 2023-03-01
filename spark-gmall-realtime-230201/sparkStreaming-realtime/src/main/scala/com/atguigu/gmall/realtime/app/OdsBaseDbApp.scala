package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author fzfor
 * @date 13:09 2023/02/12
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    val sparkConf = new SparkConf().setAppName("ods base db log").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicName = "ODS_BASE_DB_1018"
    val groupId = "ODS_BASE_DB_GROUP_1018"

    //2.从redis中读取偏移量
    val offsets = MyOffsetsUtils.readOffset(topicName, groupId)

    //3.从kafka中消费
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //4.提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5.处理数据
    //5.1 转换数据结构
    val jsonObjDStrem = offsetRangesDStream.map(
      consumerRdd => {
        val dataJson = consumerRdd.value()
        val jsonObject = JSON.parseObject(dataJson)
        jsonObject
      }
    )
    //jsonObjDStrem.print(100)

    //5.2分流

    //事实表清单
    //val factTables = Array[String]("order_info", "order_detail") //缺啥补啥
    //维度表清单
    //val dimTables = Array[String]("user_info", "base_province") //缺啥补啥

    //如何动态配置表清单?

    jsonObjDStrem.foreachRDD(
      rdd => {
        //如何动态配置表清单?
        //将表清单维护到redis中，实时任务中动态的到redis中获取表清单
        //类型：set
        //key: FACT:TABLES    DIM:TABLES
        //value:表名的集合
        //写入API：sadd
        //读取API：smembers
        //过期：不过期
        val redisFactKeys = "FACT:TABLES"
        val redisDimKeys = "DIM:TABLES"
        val jedis = MyRedisUtils.getJedisFromPool()
        //事实表清单
        val factTables = jedis.smembers(redisFactKeys)
        println("factTables" + factTables)
        //维度表清单
        val dimTables = jedis.smembers(redisDimKeys)
        println("dimTables" + dimTables)
        jedis.close()
        //注册成广播变量
        val factTablesBC = ssc.sparkContext.broadcast(factTables)
        val dimTablesBC = ssc.sparkContext.broadcast(dimTables)

        rdd.foreachPartition(
          jsonObjIter => {
            //开启redis连接
            val jedis = MyRedisUtils.getJedisFromPool()

            for (jsonObj <- jsonObjIter) {
              //提取文件操作类型
              val operType = jsonObj.getString("type")

              val opValue = operType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }

              //判断操作类型：1.明确什么操作 2.过滤不感兴趣的数据
              if (opValue != null) {
                //提取表名
                val tableName = jsonObj.getString("table")

                if (factTablesBC.value.contains(tableName)) {
                  //事实数据
                  //提取数据
                  val data = jsonObj.getString("data")
                  //DWD_ORDER_INFO_I DWD_ORDER_INFO_U DWD_ORDER_INFO_D
                  val dwdTopicName = s"DWD_${tableName.toUpperCase}_${opValue}_1018"
                  MyKafkaUtils.send(dwdTopicName, data)
                }

                if (dimTablesBC.value.contains(tableName)) {
                  //维度数据 to redis
                  //类型：string list set zset hash
                  //        hash:整个表存成一个hash，要考虑数据量大小及高频访问问题。一个kv是存在一个节点上的
                  //        hash:一条数据存成一个hash
                  //        string:一条数据存成一个jsonString
                  //key: DIM:表名:ID
                  //value: 整条数据的jsonString
                  //写入API：set
                  //读取API：get
                  //过期：不过期

                  //提取数据中的id
                  val dataObj = jsonObj.getJSONObject("data")
                  val id = dataObj.getString("id")
                  val redisKey = s"DIM:${tableName.toUpperCase}:${id}"
                  //存储到redis
                  jedis.set(redisKey, dataObj.toJSONString)

                }
              }
            }
            //关闭redis
            jedis.close()

            //刷新kafka缓冲区
            MyKafkaUtils.flush()
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)

      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
