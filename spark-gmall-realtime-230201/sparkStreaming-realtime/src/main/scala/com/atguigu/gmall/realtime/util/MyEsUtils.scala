package com.atguigu.gmall.realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import scala.collection.mutable.ListBuffer

/**
 * es工具类，用于对es进行读写操作
 *
 * @author fzfor
 * @date 14:04 2023/03/06
 */
object MyEsUtils {

  //客户端对象
  val esClient: RestHighLevelClient = build()

  //创建客户端对象
  def build() = {
    val host: String = MyPropsUtils(MyConfig.ES_HOST)
    val port: String = MyPropsUtils(MyConfig.ES_PORT)
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host, port.toInt))
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  //关闭es对象
  def close() = {
    if (esClient != null) {
      esClient.close()
    }
  }

  /**
   * 1.批写
   * 2。幂等写
   */
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]) = {
    val bulkRequest: BulkRequest = new BulkRequest(indexName)
    for ((docId, docObj) <- docs) {
      val indexRequest: IndexRequest = new IndexRequest()
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      indexRequest.source(dataJson, XContentType.JSON)
      indexRequest.id(docId)
      bulkRequest.add(indexRequest)
    }

    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }


  /**
   * 查询指定的字段
   *
   * @param indexName
   * @param fieldName
   * @return
   */
  def searchField(indexName: String, fieldName: String): List[String] = {
    //判断索引是否存在
    val getIndexRequest: GetIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean = esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!isExists) {
      return null
    }

    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest: SearchRequest = new SearchRequest(indexName)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName, null)
    searchSourceBuilder.size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceMap: util.Map[String, AnyRef] = hit.getSourceAsMap
      val mid: String = sourceMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }

  def main(args: Array[String]): Unit = {
    val list: List[String] = searchField("gmall_dau_info_1018_2022-03-28", "mid")
    println(list)
  }
}
