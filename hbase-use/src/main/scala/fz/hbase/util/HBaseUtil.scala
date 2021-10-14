package fz.hbase.util

/**
 * @author fzfor
 * @date 8:05 2021/08/26
 */
object HBaseUtil {

  def main(args: Array[String]): Unit = {
    println(getSplitForRadix(3, 10, "0", "10").mkString("|"))
  }

  /**
   * Hbase 预分区转换
   * @param region Hbase regionServer 的节点数
   * @param radix 进制 10 | 16
   * @param start 开始 => 比如：00
   * @param end 结束 => 比如：ff
   * @return Array
   */
  def getSplitForRadix(region: Int, radix: Int, start: String, end: String): Array[String] = {
    val range = start.toInt to java.lang.Long.valueOf(end, radix).toInt
    range
      .filter(_ % (range.size / region) == 0)
      .map(if (radix == 16) Integer.toHexString else _.toString)
      .tail //Hbase 左闭右开
      .toArray
  }

}
