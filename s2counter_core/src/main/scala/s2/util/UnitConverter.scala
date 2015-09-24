package s2.util

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 4. 3..
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

  def toHours(ts: Long): Long = {
    toMillis(ts) / HOUR_MILLIS * HOUR_MILLIS
  }

  val HOUR_MILLIS = 60 * 60 * 1000
}
