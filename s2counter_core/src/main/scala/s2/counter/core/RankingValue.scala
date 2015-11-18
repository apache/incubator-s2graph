package s2.counter.core

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 22..
 */

/**
 * ranking score and increment value
 * @param score ranking score
 * @param increment increment value for v1
 */
case class RankingValue(score: Double, increment: Double)

object RankingValue {
  def reduce(r1: RankingValue, r2: RankingValue): RankingValue = {
    // maximum score and sum of increment
    RankingValue(math.max(r1.score, r2.score), r1.increment + r2.increment)
  }
}
