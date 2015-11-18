package s2.counter.core

import s2.counter.core.RankingCounter.RankingValueMap
import s2.models.Counter

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 22..
 */
trait RankingStorage {
  def getTopK(key: RankingKey, k: Int): Option[RankingResult]
  def getTopK(keys: Seq[RankingKey], k: Int): Seq[(RankingKey, RankingResult)]
  def update(key: RankingKey, value: RankingValueMap, k: Int): Unit
  def update(values: Seq[(RankingKey, RankingValueMap)], k: Int): Unit
  def delete(key: RankingKey)

  def prepare(policy: Counter): Unit
  def destroy(policy: Counter): Unit
  def ready(policy: Counter): Boolean
}
