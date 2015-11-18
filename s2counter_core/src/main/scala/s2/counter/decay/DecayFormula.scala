package s2.counter.decay

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 26..
 */
trait DecayFormula {
  def apply(value: Double, millis: Long): Double
  def apply(value: Double, seconds: Int): Double
}
