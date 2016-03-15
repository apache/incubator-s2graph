package org.apache.s2graph.counter.core

import org.apache.s2graph.counter.core.RankingCounter.RankingValueMap
import org.apache.s2graph.counter.models.Counter


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
