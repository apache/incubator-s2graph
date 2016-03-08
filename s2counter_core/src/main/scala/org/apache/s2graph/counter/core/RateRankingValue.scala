package org.apache.s2graph.counter.core

case class RateRankingValue(actionScore: Double, baseScore: Double) {
  // increment score do not use.
  lazy val rankingValue: RankingValue = {
    RankingValue(actionScore / math.max(1d, baseScore), 0)
  }
}

object RateRankingValue {
  def reduce(r1: RateRankingValue, r2: RateRankingValue): RateRankingValue = {
    // maximum score and sum of increment
    RateRankingValue(math.max(r1.actionScore, r2.actionScore), math.max(r1.baseScore, r2.baseScore))
  }
}
