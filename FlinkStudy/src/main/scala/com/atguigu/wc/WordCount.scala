package com.atguigu.wc

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

/**
 * 对所有的状态统一处理
 * 方便处理，自己构建了一套类型系统，typeInformation 要处理的所有数据类型，最后都会转换保证成typeInformation
 * 主要特点：1.事件驱动 2.流的世界观 3.分层api （去和spark作比较）
 * 更重要特性：低延迟，高吞吐，正确处理乱序数据，
 * 批处理：所有数据全部到齐，然后统一处理
 * 流处理框架： 来一条处理一条
 * 读取文件每一行就是一个事件
 *
 * groupBy以后得到的不是dataSet，而是groupedDataSet
 *
 * @author fzfor
 * @date 22:03 2021/04/28
 */
//批处理word count
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inputDataSet = env.readTextFile("D:\\Study\\IDEAProjects\\Fzfor\\FlinkStudy\\src\\testData\\test.txt")
    //基于dataset做转换，首先按空格分词打散，然后按照word作为key做groupby
    val resultDataSet = inputDataSet.flatMap(_.split(" ")) //分词得到所有word构成的数据集
      .map((_, 1)) //转换成一个二元组（word，count）
      .groupBy(0) //得到groupeddataset，只有groupby 没有groupbyKey，以二元组的第一个元素作为key
      .sum(1) //聚合二元组中第二个元素的值
      //隐式转换，可以impor进scala._
      //打印输出
      resultDataSet.print()
  }
}
