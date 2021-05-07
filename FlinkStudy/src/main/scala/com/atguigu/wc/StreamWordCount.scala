package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 *
 * nc -lk 7777
 *   -l 启动一个server
 *   -k 一直保持状态
 *
 *   相当于是定义了处理的流程，有输入了以后才执行
 *   keyBy key的hashcode重分区
 *
 *   可以给不同任务设置不同的并行度
 *
 *   安装包，1.8以后的版本如果需要hadoop支持，需要单独下载hadoop支持的jar包，
 *   放入解压目录lib下
 *
 *   flink-conf.yaml文件 taskmanager控制的不仅是堆内存，还有堆外内存
 *   分配资源一般taskmanager的资源给的多，因为它是真正干活的人
 *
 *   一个taskSlot，就是一个并行任务
 *
 *   具体运行的时候并行度是根据 parallelism来配置确定 代码中配置并行度优先级最高
 *
 *   jobmanager.execution.failover-strategy:region 1.9之后添加的特性,受失败影响的最小区域恢复
 *
 *   有一些算子的并行度是无法设置的
 *
 *   同一个任务并行子任务，一个slot执行一个子任务
 *
 *   ./flink list
 *   ./flink cancel 7ee01e19d2dfa4067736d59b8d232730
 *
 *   slot之间内存是独享的，cpu不是独享的
 *   所以slot数量最好配成cpu核心数 --建议
 *
 *   并行的概念：
 *      数据并行 同一个任务，不同的的并行子任务，同时处理不同的数据
 *      任务并行 同一时间，不同的slot在执行不同的任务
 *
 *   运行slot共享，可以提供资源利用率
 *
 *   taskmanager和slot数量，绝ing了并行处理的最大能力
 *   但是不一定程序执行时都用到，程序执行时的并行度才是用到的能力
 *
 *   一般情况都是使用其他资源管理平台，k8s yarn模式
 *   yarn模式：flink需要有hadoop支持
 *
 *   1.任务是什么？一段代码到底会生成多少任务？
 *      代码中定义的每一步操作，（算子 operator）就是一个任务
 *      算子可以设置并行度，所以每一步操作可以有多个并行的子任务
 *      flink可以将前后执行的不同的任务合并起来
 *
 *   2.slot到底是什么？一段代码到底需要多少个slot来执行？
 *      slot是taskmanager拥有的计算资源的一个子集，一个任务必须在一个slot上执行
 *      每一个算子的并行任务，必须执行在不同的slot上
 *      如果是不同算子的任务，可以共享一个slot
 *
 *   3.并行度和slot数量的关系？
 *      并行度和任务有关，就是每一个算子拥有的并行任务的数量 动态概念
 *      slot数量只跟taskmananger的配置有关，代表tm比高兴处理数据的能力 静态概念
 *
 *   4.什么样的任务能够合并在一起？
 *      one to one 窄依赖 并行度相同的操作
 *
 *
 *
 *   job cancel
 * @author fzfor
 * @date 22:47 2021/04/28
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //可以从程序运行参数中读取hostname和port
    val params = ParameterTool.fromArgs(args)
    val hostname = params.get("host")
    val port = params.getInt("port")

    //接手socket文本
    val inputDataStream = env.socketTextStream(hostname, port)

    //定义转换操作：word count
    val resultDataStream = inputDataStream
      .flatMap(_.split(" ")) //以空格分词，打散得到所有的word
      .filter(_.nonEmpty)
      .map((_,1)) //转换成（word，count）二元组
      .keyBy(0) //按照第一个元素分组
      .sum(1) //按照第二个元素求和

    resultDataStream.print().setParallelism(1)

    env.execute("stream word count")
  }

}
