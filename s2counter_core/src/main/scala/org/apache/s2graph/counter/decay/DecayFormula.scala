package org.apache.s2graph.counter.decay

trait DecayFormula {
  def apply(value: Double, millis: Long): Double
  def apply(value: Double, seconds: Int): Double
}
