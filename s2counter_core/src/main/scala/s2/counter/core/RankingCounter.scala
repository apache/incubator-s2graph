package s2.counter.core

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import s2.counter.core.RankingCounter.RankingValueMap
import s2.models.Counter

import scala.collection.JavaConversions._

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 19..
 */
case class RankingRow(key: RankingKey, value: Map[String, RankingValue])
case class RateRankingRow(key: RankingKey, value: Map[String, RateRankingValue])

class RankingCounter(config: Config, storage: RankingStorage) {
  private val log = LoggerFactory.getLogger(getClass)

  val cache: LoadingCache[RankingKey, RankingResult] = CacheBuilder.newBuilder()
    .maximumSize(1000000)
    .expireAfterWrite(10l, TimeUnit.MINUTES)
    .build(
      new CacheLoader[RankingKey, RankingResult]() {
        def load(rankingKey: RankingKey): RankingResult = {
//          log.warn(s"cache load: $rankingKey")
          storage.getTopK(rankingKey, Int.MaxValue).getOrElse(RankingResult(-1, Nil))
        }
      }
    )

  def getTopK(rankingKey: RankingKey, k: Int = Int.MaxValue): Option[RankingResult] = {
    val tq = rankingKey.eq.tq
    if (TimedQualifier.getQualifiers(Seq(tq.q), System.currentTimeMillis()).head == tq) {
      // do not use cache
      storage.getTopK(rankingKey, k)
    }
    else {
      val result = cache.get(rankingKey)
      if (result.values.nonEmpty) {
        Some(result.copy(values = result.values.take(k)))
      }
      else {
        None
      }
    }
  }

  def update(key: RankingKey, value: RankingValueMap, k: Int): Unit = {
    storage.update(key, value, k)
  }

  def update(values: Seq[(RankingKey, RankingValueMap, Int)]): Unit = {
    storage.update(values)
  }

  def delete(key: RankingKey): Unit = {
    storage.delete(key)
  }

  def getAllItems(keys: Seq[RankingKey], k: Int = Int.MaxValue): Seq[String] = {
//    for {
//      key <- keys
//      result <- getTopK(key, k).toSeq
//      (item, score) <- result.values
//    } yield {
//      item
//    }
    val oldKeys = keys.filter(key => TimedQualifier.getQualifiers(Seq(key.eq.tq.q), System.currentTimeMillis()).head != key.eq.tq)
    val cached = cache.getAllPresent(oldKeys)
    val missed = keys.diff(cached.keys.toSeq)
    val found = storage.getTopK(missed, k)

//    log.warn(s"cached: ${cached.size()}, missed: ${missed.size}")

    for {
      (key, result) <- found
    } {
      cache.put(key, result)
    }

    for {
      (key, RankingResult(totalScore, values)) <- cached ++ found
      (item, score) <- values
    } yield {
      item
    }
  }.toSeq.distinct

  def prepare(policy: Counter, graphLabelOpt: Option[String]): Unit = {
    storage.prepare(policy, graphLabelOpt)
  }

  def destroy(policy: Counter): Unit = {
    storage.destroy(policy)
  }
}

object RankingCounter {
  type RankingValueMap = Map[String, RankingValue]
}