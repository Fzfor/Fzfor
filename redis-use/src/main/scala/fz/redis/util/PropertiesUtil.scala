package fz.redis.util

import java.io.{FileInputStream, IOException, InputStreamReader}
import java.util.Properties

/**
 * @author fzfor
 * @date 15:10 2021/08/25
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties = load("config.properties")
  }



  /**
   * 加载配置文件
   * @param propertieName
   * @return
   */
  def load(propertieName:String) : Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

  /**
   * 读取配置文件
   * @return
   */
  private def loadPro() = {
    var properties:Properties = null
    try {
      properties = new Properties
      //val in = new FileInputStream("D:\\JobWorkSpace\\gitAi\\ctc-zj-cyber-lbs-ldc-repository\\lbs-ldc\\lbs-taskTagFilter\\src\\main\\resources\\config.properties") //测试
      //读取与jar包相同路径下的配置文件
      val in = new FileInputStream("./config.properties")
      properties.load(in)
      in.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    properties
  }
}
