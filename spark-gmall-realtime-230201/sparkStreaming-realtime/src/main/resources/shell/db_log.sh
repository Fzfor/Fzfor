#!/bin/bash

#根据传递的日期参数修改配置文件的日期
#生成业务数据 mysql

if [ $# -ge 1 ]

then
  #替换指定位置
  sed -i "/mock.date/c mock.date: $1" /home/atguigu/spark_realtime_database_study/db_log/application.properties

fi

cd /home/atguigu/spark_realtime_database_study/db_log;java -jar gmall2020-mock-db-2021-01-22.jar >/dev/null 2>&1 &