package s2.counter.core

import s2.counter.core.RankingCounter.RankingValueMap
import s2.models.Counter

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 22..
 */
trait RankingStorage {
  def prepare(policy: Counter, rateActionOpt: Option[String])
  def destroy(policy: Counter)
  def getTopK(key: RankingKey, k: Int): Option[RankingResult]
  def getTopK(keys: Seq[RankingKey], k: Int): Seq[(RankingKey, RankingResult)]
//  def incrementBulk(key: RankingKey, value: Map[String, Double], k: Int): Unit
  def update(key: RankingKey, value: RankingValueMap, k: Int): Unit
  def update(values: Seq[(RankingKey, RankingValueMap, Int)]): Unit
  def delete(key: RankingKey)

  /**
   * ex1)
   * dimension = "age.32"
   * ex2)
   * dimension = "age.gender.32.m"
   *
   */
  protected def makeBucket(rankingKey: RankingKey): String = {
    val policyId = rankingKey.policyId
    val q = rankingKey.eq.tq.q
    val ts = rankingKey.eq.tq.ts
    val dimension = rankingKey.eq.dimension
    if (dimension.nonEmpty) {
      s"$policyId.$q.$ts.$dimension"
    }
    else {
      s"$policyId.$q.$ts"
    }
  }


  // "", "age.32", "age.gender.32.M"
  protected def makeBucketSimple(rankingKey: RankingKey): String = {
    val q = rankingKey.eq.tq.q
    val ts = rankingKey.eq.tq.ts
    val dimension = rankingKey.eq.dimension
    s"$dimension"
  }
}
