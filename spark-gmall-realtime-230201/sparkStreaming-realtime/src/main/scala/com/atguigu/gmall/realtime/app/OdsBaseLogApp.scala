package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.util
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author fzfor
 * @date 21:48 2023/02/02
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1.准备实施环境
    val sparkConf = new SparkConf()
      .setAppName("ods_base_log_app")
      .setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //从kafka消费数据
    val topic = "ODS_BASE_LOG_1018"
    val groupId = "ods_base_log_group"

    //TODO 从redis中读取offset，指定offset进行消费
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    val offsets = MyOffsetsUtils.readOffset(topic, groupId)
    if (offsets != null && offsets.nonEmpty) {
      //指定offset消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId, offsets)
    } else {
      //默认消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)
    }

    //val kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)
    //kafkaDStream.print(20)

    //TODO 从当前消费道德数据中提取offset，不对流中的数据做任何处理
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    val jsonObjDStream = offsetRangesDStream.map(
      consumerRecord => {
        val log = consumerRecord.value()
        val jsonObj = JSON.parseObject(log)
        jsonObj
      }
    )
    //jsonObjDStream.print(100)


    //分流 日志数据：
    //1.页面访问数据
    //  公共字段
    //  页面数据
    //  曝光数据
    //  事件数据
    //  错误数据
    //2.启动数据
    //  公共字段
    //  启动数据
    //  错误数据
    val DWD_PAGE_LOG_TOPIC = "DWD_PAGE_LOG_TOPIC_1018" //页面访问
    val DWD_PAGE_DISPLAY_TOPIC = "DWD_PAGE_DISPLAY_TOPIC_1018" //页面曝光
    val DWD_PAGE_ACTION_TOPIC = "DWD_PAGE_ACTION_TOPIC_1018" //页面事件
    val DWD_START_LOG_TOPIC = "DWD_START_LOG_TOPIC_1018" //启动数据
    val DWD_ERROR_LOG_TOPIC = "DWD_ERROR_LOG_TOPIC_1018" //错误数据

    //分流规则
    //  错误数据：不做任何的拆分，只要包含错误字段，直接整条数据发送到对应的topic
    //  页面数据：拆分成页面访问，曝光，事件 分别发送到对应的topic
    //  启动数据：发送到对应的topic
    jsonObjDStream.foreachRDD(
      rdd => {

        //需要在executer端批次进行kafka flush
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //分流过程
              //分流错误数据
              val errObj = jsonObj.getJSONObject("err")
              if (errObj != null) {
                //将错误数据发送到DWD_ERROR_LOG_TOPIC_1018
                MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
              } else {
                //提取公共字段
                val commonObj = jsonObj.getJSONObject("common")
                val ar = commonObj.getString("ar")
                val uid = commonObj.getString("uid")
                val os = commonObj.getString("os")
                val ch = commonObj.getString("ch")
                val isNew = commonObj.getString("is_new")
                val md = commonObj.getString("md")
                val mid = commonObj.getString("mid")
                val vc = commonObj.getString("vc")
                val ba = commonObj.getString("ba")

                //提取时间戳
                val ts = jsonObj.getLong("ts")

                //页面数据
                val pageObj = jsonObj.getJSONObject("page")
                if (pageObj != null) {
                  //提取page字段
                  val pageId = pageObj.getString("page_id")
                  val pageItem = pageObj.getString("item")
                  val duringTime = pageObj.getLong("during_time")
                  val pageItemType = pageObj.getString("item_type")
                  val lastPageId = pageObj.getString("last_page_id")
                  val sourceType = pageObj.getString("source_type")

                  //封装成PageLog
                  val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)

                  //发送到 DWD_PAGE_LOG_TOPIC topic
                  //scala case class中没有set get方法，new SerializeConfig(true)表示不基于set get方法去生成json str
                  MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                  //提取曝光数据
                  val displayJsonArr = jsonObj.getJSONArray("displays")
                  if (displayJsonArr != null && displayJsonArr.size() > 0) {
                    for (i <- 0 until displayJsonArr.size()) {
                      //循环拿到每个曝光
                      val displayJsonObj = displayJsonArr.getJSONObject(i)
                      val display_type = displayJsonObj.getString("display_type")
                      val display_item = displayJsonObj.getString("item")
                      val display_item_type = displayJsonObj.getString("item_type")
                      val display_order = displayJsonObj.getString("order")
                      val display_pos_id = displayJsonObj.getString("pos_id")

                      val pageDisplayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, sourceType, duringTime, display_type, display_item, display_item_type, display_order, display_pos_id, ts)
                      MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))

                    }

                  }

                  //提取事件数据
                  val actionJsonArr = jsonObj.getJSONArray("actions")
                  if (actionJsonArr != null && actionJsonArr.size() > 0) {
                    for (i <- 0 until actionJsonArr.size()) {
                      //循环拿到每个曝光
                      val actionJsonObj = actionJsonArr.getJSONObject(i)
                      val actionId = actionJsonObj.getString("action_id")
                      val action_item = actionJsonObj.getString("item")
                      val action_item_type = actionJsonObj.getString("item_type")
                      val action_ts = actionJsonObj.getLong("ts")
                      val pageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, sourceType, duringTime, actionId, action_item, action_item_type, action_ts, ts)
                      MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))

                    }
                  }

                } //pageObj != null ---end


                //启动数据
                val startObj = jsonObj.getJSONObject("start")
                if (startObj != null) {
                  val entry = startObj.getString("entry")
                  val loading_time = startObj.getLong("loading_time")
                  val open_ad_id = startObj.getString("open_ad_id")
                  val open_ad_ms = startObj.getLong("open_ad_ms")
                  val open_ad_skip_ms = startObj.getLong("open_ad_skip_ms")

                  val startLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, open_ad_id, loading_time, open_ad_ms, open_ad_skip_ms, ts)
                  MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))

                }

              } // (errObj != null) else -- end
            } //jsonObjIter for 循环 end

            //TODO 进行kafka 刷写
            MyKafkaUtils.flush()

          } //jsonObjIter end
        ) //rdd.foreachPartition end

        /* 原始写法
        rdd.foreach(
          jsonObj => {

          } //jsonObj => { --- end
        ) //rdd.foreach( -- end
        */


        //foreachRDD里面，foreach外面：提交offset Driver端执行，一批次执行一次（周期性）
        MyOffsetsUtils.saveOffset(topic, groupId, offsetRanges)

      } // rdd => { --- end
    ) //sonObjDStream.foreachRDD --- end

    ssc.start()
    ssc.awaitTermination()
  }

}
