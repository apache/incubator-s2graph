package s2.util

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 12..
 */
object SplitBytes {
  def apply(bytes: Array[Byte], sizes: Seq[Int]): Seq[Array[Byte]] = {
    if (sizes.sum > bytes.length) {
      throw new Exception(s"sizes.sum bigger than bytes.length ${sizes.sum} > ${bytes.length}} ")
    }

    var position = 0
    val rtn = {
      for {
        size <- sizes
      } yield {
        val slice = bytes.slice(position, position + size)
        position += size
        slice
      }
    }
    rtn ++ Seq(bytes.drop(position))
  }
}
