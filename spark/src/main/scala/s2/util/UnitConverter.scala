package s2.util

/**
 * Created by alec on 15. 4. 3..
 */
object UnitConverter {
  def toMillis(ts: Int): Long = {
    ts * 1000L
  }

  def toMillis(ts: Long): Long = {
    if (ts <= Int.MaxValue) {
      ts * 1000
    } else {
      ts
    }
  }

  def toMillis(s: String): Long = {
    toMillis(s.toLong)
  }
}
