GET _cat/nodes?v

GET /_cat/health?v

GET _cat/indices?v

GET /_cat/shards/.kibana_1

GET _cat/allocation?v

GET _cat/count?v

#数据操作
#创建索引， 不指定字段
PUT movie_index

#删除索引
DELETE movie_index

#查看索引的mapping（desc table）
GET movie_index/_mapping

#往索引中写入数据（幂等）
#幂等写入要指定docid
PUT /movie_index/_doc/1
{ "id":1,
 "name":"operation red sea",
 "doubanScore":8.5,
 "actorList":[
  {"id":1,"name":"zhang yi"},
  {"id":2,"name":"hai qing"},
  {"id":3,"name":"zhang han yu"}
  ]
}

PUT /movie_index/_doc/2
{
 "id":2,
 "name":"operation meigong river",
 "doubanScore":8.0,
 "actorList":[
  {"id":3,"name":"zhang han yu"}
  ]
}


PUT /movie_index/_doc/3
{
 "id":3,
 "name":"incident red sea",
 "doubanScore":5.0,
 "actorList":[
  {"id":4,"name":"atguigu"}
  ]
}

#查询索引中所有的doc
GET movie_index/_search

#往索引中写入数据（非幂等）
#幂等写入不指定docid
POST /movie_index/_doc
{ "id":1,
 "name":"operation red sea",
 "doubanScore":8.5,
 "actorList":[
  {"id":1,"name":"zhang yi"},
  {"id":2,"name":"hai qing"},
  {"id":3,"name":"zhang han yu"}
  ]
}

GET movie_index/_search


#实时项目中精确一次问题：
#在项目中做到了至少一次消费（会有重复）
#在写入es时，采用幂等写


#修改，整体替换
GET movie_index/_search

#直接覆盖 本质就是幂等写入
PUT /movie_index/_doc/1
{ "id":1,
 "name":"operation red sea sea",
 "doubanScore":8.5,
 "actorList":[
  {"id":1,"name":"zhang yi"},
  {"id":2,"name":"hai qing"},
  {"id":3,"name":"zhang han yu"}
  ]
}

#修改doc中的某一个字段
POST movie_index/_update/1
{
  "doc": {
    "name": "operation red sea"
  }
}


#查询一个doc
GET movie_index/_doc/2

#删除一个doc
DELETE movie_index/_doc/GafCm4YB0A-FO5f9-EVK


#分词查询
GET movie_index/_search
#搜索 "operation red sea"
GET movie_index/_search
{
  "query": {
    "match": {
      "name": "operation red sea"
    }
  }
}

# 使用query + term查询
# term后面的字段，要么字段名指定为keyword，要么value是一个不可分词的值
GET movie_index/_search
{
  "query": {
    "term": {
      "name.keyword": {
        "value": "operation red sea"
      }
    }
  }
}

GET movie_index/_search
{
  "query": {
    "term": {
      "name": {
        "value": "operation red sea"
      }
    }
  }
}


GET movie_index/_search
{
  "query": {
    "term": {
      "name": {
        "value": "sea"
      }
    }
  }
}

#解析
#倒排索引
#operation  1 2
#red  1 3
#sea  1 3
#meigong  2
#river  2
#Incident 3

# 搜索关键字 operation red sea
# operation -> 1 2
# red -> 1 3
# sea -> 1 3

# 结果
# 1->(3) 3->(2) 2->(1)
# 打分


# 分词子属性
# 搜索 "zhang yi"
GET movie_index/_search
{
  "query": {
    "match": {
      "actorList.name": "zhang yi"
    }
  }
}

GET movie_index/_mapping

# 列式存储查询
GET movie_index/_search
{
  "query": {
    "match": {
      "actorList.name.keyword": "zhang yi"
    }
  }
}



# 短语匹配（不基于分词）
GET movie_index/_search
{
  "query": {
    "match": {
      "name": "operation red"
    }
  }
}

GET movie_index/_search

GET movie_index/_search
{
  "query": {
    "match_phrase": {
      "name": "operation red"
    }
  }
}


# 条件过滤name="operation red sea"(等值判断)
GET movie_index/_search
{
  "query": {
    "match": {
      "name.keyword": "operation red sea"
    }
  }
}

GET movie_index/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "name.keyword": "operation red sea"
          }
        }
      ]
    }
  }
}



# 分词匹配”red sea” , 条件过滤 actorList.name=”zhang han yu”
# filter 必须满足
# must   必须满足，满足后才会进入到结果集
# should 不是必须满足，如果能满足会打分，如果不满足，不会打分，但都会出现到结果中

GET movie_index/_search

GET movie_index/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "actorList.name.keyword": "zhang han yu"
          }
        }
      ],
      "must": [
        {
          "match": {
            "name": "red sea"
          }
        }
      ]
    }
  }
}

GET movie_index/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "actorList.name.keyword": "zhang han yu"
          }
        }
      ],
      "should": [
        {
          "match": {
            "name": "red sea"
          }
        }
      ]
    }
  }
}



# 范围过滤
# 查询 doubanScore 大于4 小于等于 8
GET movie_index/_search

GET movie_index/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "doubanScore": {
              "gt": 4,
              "lte": 8
            }
          }
        }
      ]
    }
  }
}


# 过滤修改，将演员名字为"atguigu"的名字改为shangguigu
POST movie_index/_update_by_query
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "actorList.name": "atguigu"
          }
        }
      ]
    }
  },
  "script": {
    "source": "ctx._source['actorList'][0]['name']=params.newName",
    "params": {
      "newName":"shangguigu"
    },
    "lang": "painless"
  }
}


POST movie_index/_update_by_query
{
  "query": {
    "term": {
      "actorList.name": {
        "value": "shangguigu"
      }
    }
  },
  "script": {
    "source": "ctx._source['actorList'][0]['name']=params.newName",
    "params": {
      "newName":"shangguigu111"
    },
    "lang": "painless"
  }
}


# 过滤删除
# 演员列表中包含zhang han yu
GET movie_index/_search

POST movie_index/_delete_by_query
{
  "query": {
    "match": {
      "actorList.name": "zhang han yu"
    }
  }
}



# 排序, 查询所有数据，按照doubanScoure排序 asc desc
GET movie_index/_search
{
  "sort": [
    {
      "doubanScore": {
        "order": "asc"
      }
    }
  ]
}

# 查询演员列表中包含zhang han yu的数据，按照doubanScore升序排序
GET movie_index/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "actorList.name.keyword": "zhang han yu"
          }
        }
      ]
    }
  }
  ,
  "sort": [
    {
      "doubanScore": {
        "order": "asc"
      }
    }
  ]
}


GET movie_index/_search
{
  "query": {
    "match": {
      "actorList.name": "zhang han yu"
    }
  }
  ,
  "sort": [
    {
      "doubanScore": {
        "order": "asc"
      }
    }
  ]
}

GET movie_index/_search
{
  "query": {
    "term": {
      "actorList.name.keyword": {
        "value": "zhang han yu"
      }
    }
  }
  ,
  "sort": [
    {
      "doubanScore": {
        "order": "asc"
      }
    }
  ]
}


# 分页查询
# 当前页码：pageNum
# 每页条数：pageSize
# 查的数据：（pageNum - 1）* pageSize
# 例如：每页显示2条数据
# 第1页：0  1  （1-1）* 2 = 0
# 第2页：2  3  （2-1）* 2 = 2
# 第3页：4  5  （3-1）* 2 = 4
# mysql：limit ?, ?
GET movie_index/_search
{
  "from": 0
  ,
  "size": 2
}


# 高亮
# 分词匹配 red sea
GET movie_index/_search
{
  "query": {
    "match": {
      "name": "red sea"
    }
  }
  ,
  "highlight": {
    "fields": {
      "name": {}
    }
  }
}


# 聚合
# 取出每个演员共参演了多少部电影
# 组内count （白送）
GET movie_index/_search
{
  "aggs": {
    "group_by_actor_name": {
      "terms": {
        "field": "actorList.name.keyword",
        "size": 3
      }
    }
  }
}



# 每个演员参演电影的平均分是多少，并按平均分降序排序
# 分组
# 组内avg
# 排序
GET movie_index/_search
{
  "aggs": {
    "group_by_actor_name": {
      "terms": {
        "field": "actorList.name.keyword",
        "size": 10 ,
        "order": {
          "doubanScoreAvg": "desc"
        }
      }
      ,
      "aggs": {
        "doubanScoreAvg": {
          "avg": {
            "field": "doubanScore"
          }
        }
      }
    }
  }
  ,
  "size": 0
}


# sql
# 每个演员参演电影的平均分是多少，并按平均分降序排序
# 分组
# 组内avg
# 排序
GET _sql?format=txt
{
  "query":
  """
  SELECT
    actorList.name.keyword,
    avg(doubanScore) dbs_avg
  FROM movie_index
  group by actorList.name.keyword
  order by dbs_avg desc
  """
}



GET _sql?format=txt
{
  "query":
  """
  SELECT
    actorList.name.keyword,
    avg(doubanScore) dbs_avg
  FROM movie_index
  where match(name, 'red sea')
  group by actorList.name.keyword
  order by dbs_avg desc
  """
}


# 中文分词
# 没有提前创建索引，直接插入数据，es会自动创建索引
PUT /movie_index_cn/_doc/1
{ "id":1,
 "name":"红海行动",
 "doubanScore":8.5,
 "actorList":[
  {"id":1,"name":"张译"},
  {"id":2,"name":"海清"},
  {"id":3,"name":"张涵予"}
  ]
}

PUT /movie_index_cn/_doc/2
{
 "id":2,
 "name":"湄公河行动",
 "doubanScore":8.0,
 "actorList":[
  {"id":3,"name":"张涵予"}
  ]
}

PUT /movie_index_cn/_doc/3
{
 "id":3,
 "name":"红海事件",
 "doubanScore":5.0,
 "actorList":[
  {"id":4,"name":"尚硅谷"}
  ]
}

GET movie_index_cn/_search



GET movie_index_cn/_search
{
  "query": {
    "match": {
      "name": "红海故事"
    }
  }
}

GET movie_index_cn/_search
{
  "query": {
    "match": {
      "name": "上海银行"
    }
  }
}


# ES默认对中文的分词
# 默认中文的分词是按照字进行拆分
GET _analyze
{
  "text": "上海银行"
}


# 中文分词器 ik
# ik_smart
# ik_max_word
GET _analyze
{
  "text": "红海行动",
  "analyzer": "ik_smart"
}

GET _analyze
{
  "text": "红海行动",
  "analyzer": "ik_max_word"
}

GET _analyze
{
  "text": "我是中国人",
  "analyzer": "ik_smart"
}

GET _analyze
{
  "text": "我是中国人",
  "analyzer": "ik_max_word"
}

GET movie_index_cn/_mapping

DELETE movie_index_cn

GET movie_index_cn/_search


PUT movie_index_cn
{
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "id":{
        "type": "long"
      },
      "name":{
        "type": "text"
        ,
        "analyzer": "ik_smart"
      },
      "doubanScore":{
        "type": "double"
      },
      "actorList":{
        "properties": {
          "id":{
            "type":"long"
          },
          "name":{
            "type":"keyword"
          }
        }
      }
    }
  }
}


# 索引别名
GET movie_index/_search
GET movie_index_cn/_search

# 给现有的索引取别名
POST _aliases
{
  "actions": [
    {
      "add": {
        "index": "movie_index",
        "alias": "movie_index_2023"
      }
    }
  ]
}

POST _aliases
{
  "actions": [
    {
      "add": {
        "index": "movie_index_cn",
        "alias": "movie_index_2023"
      }
    }
  ]
}

GET movie_index_2023/_search


# 给索引取别名好像也挺麻烦?
# 创建索引直接指定别名
DELETE movie_index_1

PUT movie_index_1
{
  "aliases": {
    "movie_index_1_20210101": {},
    "goudan": {}
  },
  "mappings": {
    "properties": {
      "id":{
        "type": "long"
      },
      "name":{
        "type": "text"
        ,
        "analyzer": "ik_smart"
      },
      "doubanScore":{
        "type": "double"
      },
      "actorList":{
        "properties": {
          "id":{
            "type":"long"
          },
          "name":{
            "type":"keyword"
          }
        }
      }
    }
  }
}

PUT /movie_index_1/_doc/1
{ "id":1,
 "name":"红海行动",
 "doubanScore":8.5,
 "actorList":[
  {"id":1,"name":"张译"},
  {"id":2,"name":"海清"},
  {"id":3,"name":"张涵予"}
  ]
}

GET goudan/_search



# 给索引的子集创建视图（别名）
GET movie_index/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "doubanScore": {
              "gte": 5,
              "lte": 8
            }
          }
        }
      ]
    }
  }
}


POST _aliases
{
  "actions": [
    {
      "add": {
        "index": "movie_index",
        "alias": "movie_index_dbs",
        "filter": {
          "range": {
            "doubanScore": {
              "gte": 5,
              "lte": 8
            }
          }
        }
      }
    }
  ]
}

GET movie_index_dbs/_search



# 索引无缝切换
# java -> movie2022 -> movie_index
# 从movie_index上移除movie2022别名
# 把movie2022别名加到movie_index_cn上

POST _aliases
{
  "actions": [
    {
      "add": {
        "index": "movie_index",
        "alias": "movie2022"
      }
    }
  ]
}

GET movie2022/_search

# 原子操作
POST /_aliases
{
 "actions": [
   {
     "remove": {
       "index": "movie_index",
       "alias": "movie2022"
     }
   },
   {
     "add": {
       "index": "movie_index_cn",
       "alias": "movie2022"
     }
   }
 ]
}

GET movie2022/_search

# 查询别名列表
GET _cat/aliases?v

# 索引模板
PUT _template/template_movie2023
{
  "index_patterns": ["movie_test*"],
  "settings": {
    "number_of_shards": 1
  },
  "aliases" : {
    "{index}-query": {},
    "movie_test-query":{}
  },
  "mappings": {
    "properties": {
      "id": {
        "type": "keyword"
      },
      "movie_name": {
        "type": "text",
        "analyzer": "ik_smart"
      }
    }
  }
}


PUT movie_test2023-03-02/_doc/3
{
  "id": "333",
  "movie_name":"速度与激情",
  "actor_name":"古天乐"
}


GET movie_test2023-03-02/_search
GET movie_test-query/_search
GET movie_test2023-03-02/_mapping
GET movie_index/_mapping

# 查看所有模板
GET _cat/templates?v


# 查看某个模板详情
GET _template/template_movie2023


# 主动触发合并操作：
POST movie_index/_forcemerge?max_num_segments=1

# 查看索引的段情况
GET _cat/indices/?s=segmentsCount:desc&v&h=index,segmentsCount,segmentsMemory,memoryTotal,storeSize,p,r


GET movie_test/_search

GET movie0304/_search

GET movie0304/_mapping




# search :
# * 查询 doubanScore>=5.0 关键词搜索 red sea
# * 关键词高亮显示
# * 显示第一页，每页 2 条
# * 按 doubanScore 从大到小排序
GET movie_index/_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "doubanScore": {
              "gte": 5.0
            }
          }
        }
      ]
      ,
      "must": [
        {
          "match": {
            "name": "red sea"
          }
        }
      ]
    }
  }
  ,
  "highlight": {
    "fields": {
      "name": {}
    }
  }
  ,
  "from": 0
  ,
  "size": 1
  ,
  "sort": [
    {
      "doubanScore": {
        "order": "desc"
      }
    }
  ]

}


# 查询每位演员参演的电影的平均分，倒叙排序
GET movie_index/_search
{
  "aggs": {
    "groupByName": {
      "terms": {
        "field": "actorList.name.keyword",
        "size": 10,
        "order": {
          "doubanScoreAvg": "desc"
        }
      }
      ,
      "aggs": {
        "doubanScoreAvg": {
          "avg": {
            "field": "doubanScore"
          }
        }
      }
    }
  }
  ,
  "size": 0
}



#实时数仓日活索引模板
PUT _template/gmall_dau_info_template
{
 "index_patterns": ["gmall_dau_info*"],
 "settings": {
 "number_of_shards": 3,
 "index.max_result_window" :"5000000"
 },
 "aliases" : {
 "{index}-query": {},
 "gmall_dau_info_all":{}
 },
 "mappings": {
 "properties":{
 "mid":{
 "type":"keyword"
 },
 "user_id":{
 "type":"keyword"
 },
 "province_id":{
 "type":"keyword"
 },
 "channel":{
 "type":"keyword"
 },
 "is_new":{
 "type":"keyword"
 },
 "model":{
 "type":"keyword"
 },
 "operate_system":{
 "type":"keyword"
 },
 "version_code":{
 "type":"keyword"
 },
 "page_id":{
 "type":"keyword"
 },
 "page_item":{
 "type":"keyword"
 },
 "page_item_type":{
 "type":"keyword"
 },
 "during_time":{
 "type":"long"
 },
 "user_gender":{
 "type":"keyword"
 },
 "user_age":{
 "type":"integer"
 },
 "province_name":{
 "type":"keyword"
 },
 "province_iso_code":{
 "type":"keyword"
 },
 "province_3166_2":{
 "type":"keyword"
 },
 "province_area_code":{
 "type":"keyword"
 },
 "dt":{
 "type":"keyword"
 },
  "hr":{
 "type":"keyword"
 },
 "ts":{
 "type":"date"
 }
 }
 }
}

GET gmall_dau_info_1018_2022-03-28/_search

GET gmall_dau_info_all/_search

GET _cat/indices/?v

GET _cat/aliases/?v




#实时数仓宽表索引模板
PUT _template/gmall_order_wide_template
{
 "index_patterns": ["gmall_order_wide*"],
 "settings": {
 "number_of_shards": 3
 },
 "aliases" : {
 "{index}-query": {},
 "gmall_order_wide-query":{}
 },
 "mappings" : {
 "properties" : {
 "detail_id" : {
 "type" : "keyword"
 },
 "order_id" : {
 "type" : "keyword"
 },
 "sku_id" : {
 "type" : "keyword"
 },
 "sku_num" : {
 "type" : "long"
 },
 "sku_name" : {
 "type" : "text",
 "analyzer": "ik_max_word"
 },
 "order_price" : {
 "type" : "float"
 },
 "split_total_amount" : {
 "type" : "float"
 },
 "split_activity_amount" : {
 "type" : "float"
 },
 "split_coupon_amount" : {
 "type" : "float"
 },
 "province_id" : {
 "type" : "keyword"
 },
 "order_status" : {
 "type" : "keyword"
 },
 "user_id" : {
 "type" : "keyword"
 },
 "total_amount" : {
 "type" : "float"
 },
 "activity_reduce_amount" : {
 "type" : "float"
 },
 "coupon_reduce_amount" : {
 "type" : "float"
 },
 "original_total_amount" : {
 "type" : "float"
 },
 "feight_fee" : {
 "type" : "float"
 },
 "feight_fee_reduce" : {
 "type" : "float"
 },
 "expire_time" : {
 "type" : "date" ,
 "format" : "yyyy-MM-dd HH:mm:ss"
 },
 "refundable_time" : {
 "type" : "date" ,
 "format" : "yyyy-MM-dd HH:mm:ss"
 },
 "create_time" : {
 "type" : "date" ,
 "format" : "yyyy-MM-dd HH:mm:ss"
 },
 "operate_time" : {
 "type" : "date" ,
 "format" : "yyyy-MM-dd HH:mm:ss"
 },
 "create_date" : {
 "type" : "keyword"
 },
 "create_hour" : {
 "type" : "keyword"
 },
 "province_name" : {
 "type" : "keyword"
 },
 "province_area_code" : {
 "type" : "keyword"
 },
 "province_3166_2_code" : {
 "type" : "keyword"
 },
 "province_iso_code" : {
 "type" : "keyword"
 },
 "user_age" : {
 "type" : "long"
 },
 "user_gender" : {
 "type" : "keyword"
 }
 }
 }
 }


GET gmall_order_wide-query/_search


GET gmall_dau_info_1018_2022-03-28/_search
{
  "_source": "mid"
}


#修改配置扩大返回结果数，如下：
PUT /_settings
{
 "index.max_result_window" :"5000000"
}


GET gmall_dau_info_1018_2023-03-08/_search

GET gmall_order_wide_1018_2023-03-08/_search

GET gmall_dau_info_1018_2022-03-28/_search
{
  "size": 0
}

GET gmall_dau_info_1018_2022-03-28/_search
{
  "aggs": {
    "gruopbyhr": {
      "terms": {
        "field": "hr",
        "size": 24
      }
    }
  }
  ,
  "size": 0
}

#因为会进行分词，有小米  手机都会搜索出来 17条
GET gmall_order_wide_1018_2022-03-29/_search
{
  "query": {
    "match": {
      "sku_name": "小米手机"
    }
  }
}


#既要有小米，也要有手机，同时满足 8条
GET gmall_order_wide_1018_2022-03-29/_search
{
  "query": {
    "match": {
      "sku_name": {
        "query": "小米手机",
        "operator": "and"
      }
    }
  }
  ,
  "aggs": {
    "groupbygender": {
      "terms": {
        "field": "user_gender",
        "size": 2
      }
      ,
      "aggs": {
        "totalamount": {
          "sum": {
            "field": "split_total_amount"
          }
        }
      }
    }
  }
  ,
  "size": 0
}


GET gmall_order_wide_1018_2022-03-29/_search
{
  "_source": ["create_time",
              "order_price",
              "province_name",
              "sku_name",
              "sku_num",
              "total_amount",
              "user_age",
              "user_gender"]
  ,
  "query": {
    "match": {
      "sku_name": {
        "query": "小米手机",
        "operator": "and"
      }
    }
  }
  ,
  "from": 0
  ,
  "size": 2
  ,
  "highlight": {
    "fields": {
      "sku_name": {}
    }
  }
}

















