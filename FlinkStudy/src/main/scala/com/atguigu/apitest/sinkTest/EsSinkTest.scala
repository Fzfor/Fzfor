package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

/**
 * @author fzfor
 * @date 7:47 2021/05/11
 */
object EsSinkTest {
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

    //定义一个httohosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200))
    //定义一个ElasticsearchSinkFunction
    val esSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //要存es，相当于就是发送http请求,
        //定义包装写入es的数据
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("sensor_id", t.id)
        dataSource.put("temp", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)

        //创建一个index request
        val indexRequest = Requests.indexRequest().index("sensor_temp")
          .`type`("readingdata")
          .source(dataSource)
        //用indexer发送http请求
        requestIndexer.add(indexRequest)
        println(t + " saved successfully")
      }
    }

    dataStream.addSink( new ElasticsearchSink.Builder[SensorReading](httpHosts,esSinkFunc).build() )

    env.execute("es sink test")

  }

}
