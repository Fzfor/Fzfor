package com.atguigu.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilders, RangeQueryBuilder, TermQueryBuilder}
import org.elasticsearch.index.reindex.{UpdateByQueryAction, UpdateByQueryRequest}
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.{Aggregation, AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

import java.util



/**
 * @author fzfor
 * @date 17:06 2023/03/03
 */
object EsTest {


  def main(args: Array[String]): Unit = {
    //println(client)

    //put()
    //post()
    //bulk()
    //update()
    //updateByQuery()
    //delete()
    //getById()
    //searchByFilter()
    searchByAggs()

    close()
  }


  /**
   * 查询 - 单条查询
   */
  def getById() = {
    val getRequest: GetRequest = new GetRequest("movie0304", "1001")
    val getResponse: GetResponse = client.get(getRequest, RequestOptions.DEFAULT)
    val dataStr: String = getResponse.getSourceAsString
    println(dataStr)
  }

  /**
   * 查询 - 条件查询
   * search :
   *
   * 查询 doubanScore>=5.0 关键词搜索 red sea
   * 关键词高亮显示
   * 显示第一页，每页 1 条
   * 按 doubanScore 从大到小排序
   */
  def searchByFilter()={
    val searchRequest: SearchRequest = new SearchRequest("movie_index")
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    //query
    //filter range
    val doubanRangeQueryBuilder: RangeQueryBuilder = QueryBuilders.rangeQuery("doubanScore")
    doubanRangeQueryBuilder.gte(5.0)
    //must match
    val nameMatchQueryBuilder: MatchQueryBuilder = QueryBuilders.matchQuery("name", "red sea")
    //bool
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    boolQueryBuilder.filter(doubanRangeQueryBuilder)
    boolQueryBuilder.must(nameMatchQueryBuilder)
    searchSourceBuilder.query(boolQueryBuilder)
    //分页
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(1)
    //排序
    searchSourceBuilder.sort("doubanScore", SortOrder.DESC)
    //高亮
    val highlightBuilder: HighlightBuilder = new HighlightBuilder()
    highlightBuilder.field("name")
    searchSourceBuilder.highlighter(highlightBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    //获取总条数
    val totalDocs: Long = searchResponse.getHits.getTotalHits.value
    //明细
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      //数据
      val dataJson: String = hit.getSourceAsString
      //提取高亮
      val highlightFields: util.Map[String, HighlightField] = hit.getHighlightFields
      val highlightField: HighlightField = highlightFields.get("name")
      val fragments: Array[Text] = highlightField.getFragments
      val highlightValue: String = fragments(0).toString

      println("明细数据:" + dataJson)
      println("高亮:" + highlightValue)
    }

  }

  /**
   * 查询 - 聚合查询
   * 查询每位演员参演的电影的平均分，倒叙排序
   */
  def searchByAggs() = {
    val searchRequest: SearchRequest = new SearchRequest("movie_index")
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    //不要明细
    searchSourceBuilder.size(0)
    //group
    val termsAggregationBuilder: TermsAggregationBuilder = AggregationBuilders.terms("groupByActorName")
      .field("actorList.name.keyword")
      .size(10)
      .order(BucketOrder.aggregation("doubanScoreAvg", false))
    //avg
    val doubanScoreAvgAggregationBuilder: AvgAggregationBuilder = AggregationBuilders.avg("doubanScoreAvg").field("doubanScore")
    termsAggregationBuilder.subAggregation(doubanScoreAvgAggregationBuilder)

    searchSourceBuilder.aggregation(termsAggregationBuilder)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    val aggregations: Aggregations = searchResponse.getAggregations
    //val groupByActorNameAggregation: Aggregation = aggregations.get[Aggregation]("groupByActorName")
    val groupByActorNameParsedTerms: ParsedTerms = aggregations.get[ParsedTerms]("groupByActorName")
    val buckets: util.List[_ <: Terms.Bucket] = groupByActorNameParsedTerms.getBuckets
    import scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      //演员名字
      val actorName: String = bucket.getKeyAsString
      //电影数
      val movieCount: Long = bucket.getDocCount
      //平均分
      val subAggregations: Aggregations = bucket.getAggregations
      val doubanScoreAvg: ParsedAvg = subAggregations.get[ParsedAvg]("doubanScoreAvg")
      val avgScore: Double = doubanScoreAvg.getValue
      println(s"演员名字：$actorName，一共出演了${movieCount}部电影，平均分为${avgScore}")
      //println("平均分：" + avgScore)
    }c
  }


  /**
   * 删除
   * @return
   */
  def delete() = {
    val deleteRequest: DeleteRequest = new DeleteRequest("movie0304", "HtEeq4YB1r56wd3ZhXDU")

    client.delete(deleteRequest, RequestOptions.DEFAULT)
  }


  /**
   * 修改 - 单条修改
   */
  def update() = {
    val updateRequest: UpdateRequest = new UpdateRequest("movie0304", "1001")
    updateRequest.doc("movie_name", "功夫")
    client.update(updateRequest, RequestOptions.DEFAULT)
  }


  /**
   * 修改 - 条件修改
   */
  def updateByQuery() = {
    val updateByQueryRequest: UpdateByQueryRequest = new UpdateByQueryRequest("movie0304")
    //query
    //val termQueryBuilder: TermQueryBuilder = new TermQueryBuilder("movie_name.keyword", "速度与激情2")
    val termQueryBuilder: TermQueryBuilder = QueryBuilders.termQuery("movie_name.keyword", "速度与激情2")
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    boolQueryBuilder.filter(termQueryBuilder)
    updateByQueryRequest.setQuery(boolQueryBuilder)

    //update
    val params: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    params.put("newName", "速度与激情2222")
    val script: Script = new Script(
      ScriptType.INLINE,
      Script.DEFAULT_SCRIPT_LANG,
      "ctx._source['movie_name']=params.newName",
      params
    )
    updateByQueryRequest.setScript(script)

    client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT)
  }


  /**
   * 批量写
   */
  def bulk() = {
    val bulkRequest: BulkRequest = new BulkRequest()

    val movies: List[Movie] = List[Movie](
      Movie("1002", "长津湖"),
      Movie("1003", "水门桥"),
      Movie("1004", "狙击手"),
      Movie("1005", "熊出没")
    )
    for (movie <- movies) {
      val indexRequest: IndexRequest = new IndexRequest("movie0304")
      val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
      indexRequest.source(movieJson, XContentType.JSON)
      //幂等写指定doc id，非幂等不指定doc id
      indexRequest.id(movie.id)
      //将indexRequest加入到bulk
      bulkRequest.add(indexRequest)
    }

    client.bulk(bulkRequest, RequestOptions.DEFAULT)
  }


  /**
   * 增，幂等，指定docid
   */
  def put() = {
    val indexRequest: IndexRequest = new IndexRequest()
    //指定索引
    indexRequest.index("movie0304")
    //指定doc
    //val movie: Movie = Movie("1001", "速度与激情")
    val movie: Movie = Movie("1001", "速度与激情2")
    val docJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(docJson, XContentType.JSON)
    //指定doc id
    indexRequest.id("1001")
    client.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * 增，非幂等，不指定docid
   */
  def post() = {
    val indexRequest: IndexRequest = new IndexRequest()
    //指定索引
    indexRequest.index("movie0304")
    //指定doc
    val movie: Movie = Movie("1001", "速度与激情2")
    //val movie: Movie = Movie("1001", "速度与激情789")
    val docJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(docJson, XContentType.JSON)

    //指定doc id
    //indexRequest.id("1001")

    client.index(indexRequest, RequestOptions.DEFAULT)
  }






  //客户端对象
  var client: RestHighLevelClient = create()

  //创建客户端对象
  def create() = {
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost("hadoop102", 9200))

    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  //关闭客户端对象
  def close() = {
    if (client != null) {
      client.close()
    }
  }
}


case class Movie(id: String, movie_name: String)