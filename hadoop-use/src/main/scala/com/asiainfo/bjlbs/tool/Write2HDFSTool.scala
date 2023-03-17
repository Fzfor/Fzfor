package com.asiainfo.bjlbs.tool

import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
/**
 * @author fzfor
 * @date 11:07 2022/06/13
 */
class Write2HDFSTool {
  var fileSystem :FileSystem = null

  def initialize: Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://bjdxyzbd")
    fileSystem = FileSystem.get(conf)
  }

  def uploadFile(src:String, dst: String, day: String, hour: String) = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst + "/clndr_dt_id=" + day + "/hour_id=" + hour)

    fileSystem.copyFromLocalFile(false,srcPath,dstPath)
  }

  def close: Unit = {
    if (fileSystem != null) {
      fileSystem.close()
    }
  }

}
