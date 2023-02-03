package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

/**
 * 配置文件解析类
 *
 * @author fzfor
 * @date 15:49 2023/02/01
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String): String = {
    bundle.getString(propsKey)
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils.apply(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
  }
}
