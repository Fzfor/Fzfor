package com.asiainfo.bjlbs.app

import com.asiainfo.bjlbs.tool.{FileTool, Write2HDFSTool}

import java.util.concurrent.{Callable, Executors}

/**
 * @author fzfor
 * @date 10:49 2022/06/13
 */
object Write2HDFSApp {
  def main(args: Array[String]): Unit = {
    val src = args(0)
    val dst = args(1)
    val poolNum = args(2).toInt
    val day = args(3)
    val hour = args(4)

    //获取src中需要上传的文件
    val files = FileTool.getFilesArr(src)

    //初始化hdfs file system
    val hdfsTool = new Write2HDFSTool
    hdfsTool.initialize

    val executorService = Executors.newScheduledThreadPool(poolNum)

    for (file <- files) {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          hdfsTool.uploadFile(file.toString, dst, day, hour)
        }
      })
    }

    //等待解析完成
    executorService.shutdown()
    while (!executorService.isTerminated()){}

    println("put done!")


    hdfsTool.close
  }

}
