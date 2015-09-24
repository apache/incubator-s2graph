package s2.counter.decay

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 26..
 */
case class ExpDecayFormula(halfLifeInMillis: Double) extends DecayFormula {
  val decayRate = - Math.log(2) / halfLifeInMillis

  override def apply(value: Double, millis: Long): Double = {
    value * Math.pow(Math.E, decayRate * millis)
  }

  override def apply(value: Double, seconds: Int): Double = {
    apply(value, seconds * 1000L)
  }
}

object ExpDecayFormula {
  @deprecated("do not use. just experimental", "0.14")
  def byWindowTime(windowInMillis: Long, pct: Double): ExpDecayFormula = {
    val halfLife = windowInMillis * Math.log(0.5) / Math.log(pct)
    ExpDecayFormula(halfLife)
  }

  def byMeanLifeTime(millis: Long): ExpDecayFormula = {
    ExpDecayFormula(millis * Math.log(2))
  }
}
