package benchmark

import org.specs2.mutable.Specification

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 6..
  */
trait BenchmarkCommon extends Specification {
  val wrapStr = s"\n=================================================="

  def duration[T](prefix: String = "")(block: => T) = {
    val startTs = System.currentTimeMillis()
    val ret = block
    val endTs = System.currentTimeMillis()
    println(s"$wrapStr\n$prefix: took ${endTs - startTs} ms$wrapStr")
    ret
  }

  def durationWithReturn[T](prefix: String = "")(block: => T): (T, Long) = {
    val startTs = System.currentTimeMillis()
    val ret = block
    val endTs = System.currentTimeMillis()
    val duration = endTs - startTs
//    println(s"$wrapStr\n$prefix: took $duration ms$wrapStr")
    (ret, duration)
  }
}
