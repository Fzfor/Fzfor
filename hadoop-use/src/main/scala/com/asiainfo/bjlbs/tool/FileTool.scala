package com.asiainfo.bjlbs.tool

import java.io.File

/**
 * @author fzfor
 * @date 14:22 2022/06/13
 */
object FileTool {

  def main(args: Array[String]): Unit = {
    getFilesArr("D:\\AsiaInfoJob\\20210524-OIDD\\02-合并小文件")
    //println("sdf")
  }

  def getFilesArr(path: String) = {
    val folder = new File(path)
    val files = folder.listFiles()
    //if (files != null && files.length > 0) {
    //  for (file <-files) {
    //    println(file.toString)
    //  }
    //}
    files
  }
}
