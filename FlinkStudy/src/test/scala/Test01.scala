/**
 * @author fzfor
 * @date 17:23 2021/07/05
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    val datas = "1|2|3|4|5|6".split("\\|")

    for (index <- 3 until datas.length) {
      println(index)
    }
  }
}
