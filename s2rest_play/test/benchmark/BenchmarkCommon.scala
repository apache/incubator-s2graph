package benchmark

import org.specs2.mutable.Specification

trait BenchmarkCommon extends Specification {
  val wrapStr = s"\n=================================================="

  def duration[T](prefix: String = "")(block: => T) = {
    val startTs = System.currentTimeMillis()
    val ret = block
    val endTs = System.currentTimeMillis()
    println(s"$wrapStr\n$prefix: took ${endTs - startTs} ms$wrapStr")
    ret
  }
}
