/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package s2.counter.core

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core.TimedQualifier.IntervalUnit.IntervalUnit
import s2.counter.decay.ExpDecayFormula
import s2.counter.{TrxLog, TrxLogResult}
import s2.models.Counter
import s2.util.{CollectionCache, CollectionCacheConfig, FunctionParser}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 11..
 */

case class ExactCounterRow(key: ExactKeyTrait, value: Map[ExactQualifier, Long])

case class FetchedCounts(exactKey: ExactKeyTrait, qualifierWithCountMap: Map[ExactQualifier, Long])
case class DecayedCounts(exactKey: ExactKeyTrait, qualifierWithCountMap: Map[ExactQualifier, Double])

case class FetchedCountsGrouped(exactKey: ExactKeyTrait, intervalWithCountMap: Map[(IntervalUnit, Map[String, String]), Map[ExactQualifier, Long]])

class ExactCounter(config: Config, storage: ExactStorage) {
  import ExactCounter._

  val syncDuration = Duration(10, SECONDS)
  private val log = LoggerFactory.getLogger(getClass)

  val storageStatusCache = new CollectionCache[Option[Boolean]](CollectionCacheConfig(1000, 60, negativeCache = false, 60))
  
  // dimension: age, value of ages
  def getCountAsync(policy: Counter,
                    itemId: String,
                    intervals: Seq[IntervalUnit],
                    from: Long,
                    to: Long,
                    dimension: Map[String, Set[String]])
                   (implicit ex: ExecutionContext): Future[Option[FetchedCountsGrouped]] = {
    for {
      fetchedCounts <- getCountsAsync(policy, Seq(itemId),
        intervals.map(interval => (TimedQualifier(interval, from), TimedQualifier(interval, to))), dimension)
    } yield {
      fetchedCounts.headOption
    }
  }

  // multi item, time range and multi dimension
  def getCountsAsync(policy: Counter,
                     items: Seq[String],
                     timeRange: Seq[(TimedQualifier, TimedQualifier)],
                     dimQuery: Map[String, Set[String]])
                    (implicit ex: ExecutionContext): Future[Seq[FetchedCountsGrouped]] = {
    storage.get(policy, items, timeRange, dimQuery)
  }

  def getCount(policy: Counter,
               itemId: String,
               intervals: Seq[IntervalUnit],
               from: Long,
               to: Long,
               dimension: Map[String, Set[String]])
              (implicit ex: ExecutionContext): Option[FetchedCountsGrouped] = {
    getCounts(policy, Seq(itemId),
      intervals.map(interval => (TimedQualifier(interval, from), TimedQualifier(interval, to))), dimension).headOption
  }

  def getCount(policy: Counter,
               itemId: String,
               intervals: Seq[IntervalUnit],
               from: Long,
               to: Long)
              (implicit ex: ExecutionContext): Option[FetchedCounts] = {
    val future = storage.get(policy,
      Seq(itemId),
      intervals.map(interval => (TimedQualifier(interval, from), TimedQualifier(interval, to))))
    Await.result(future, syncDuration).headOption
  }

  // multi item, time range and multi dimension
  def getCounts(policy: Counter,
                items: Seq[String],
                timeRange: Seq[(TimedQualifier, TimedQualifier)],
                dimQuery: Map[String, Set[String]])
               (implicit ex: ExecutionContext): Seq[FetchedCountsGrouped] = {
    Await.result(storage.get(policy, items, timeRange, dimQuery), syncDuration)
  }

  def getRelatedCounts(policy: Counter, keyWithQualifiers: Seq[(String, Seq[ExactQualifier])])
                      (implicit ex: ExecutionContext): Map[String, Map[ExactQualifier, Long]] = {
    val queryKeyWithQualifiers = {
      for {
        (itemKey, qualifiers) <- keyWithQualifiers
      } yield {
        val relKey = ExactKey(policy.id, policy.version, policy.itemType, itemKey)
        (relKey, qualifiers)
      }
    }
    val future = storage.get(policy, queryKeyWithQualifiers)

    for {
      FetchedCounts(exactKey, exactQualifierToLong) <- Await.result(future, syncDuration)
    } yield {
      exactKey.itemKey -> exactQualifierToLong
    }
  }.toMap

  def getPastCounts(policy: Counter, keyWithQualifiers: Seq[(String, Seq[ExactQualifier])])
                   (implicit ex: ExecutionContext): Map[String, Map[ExactQualifier, Long]] = {
    // query paste count
    val queryKeyWithQualifiers = {
      for {
        (itemKey, qualifiers) <- keyWithQualifiers
      } yield {
        val relKey = ExactKey(policy.id, policy.version, policy.itemType, itemKey)
        (relKey, qualifiers.map(eq => eq.copy(tq = eq.tq.add(-1))))
      }
    }
    val future = storage.get(policy, queryKeyWithQualifiers)

    for {
      FetchedCounts(exactKey, exactQualifierToLong) <- Await.result(future, syncDuration)
    } yield {
      // restore tq
      exactKey.itemKey -> exactQualifierToLong.map { case (eq, v) =>
        eq.copy(tq = eq.tq.add(1)) -> v
      }
    }
  }.toMap

  def getDecayedCountsAsync(policy: Counter,
                            items: Seq[String],
                            timeRange: Seq[(TimedQualifier, TimedQualifier)],
                            dimQuery: Map[String, Set[String]],
                            qsSum: Option[String])(implicit ex: ExecutionContext): Future[Seq[DecayedCounts]] = {
    val groupedTimeRange = timeRange.groupBy(_._1.q)
    getCountsAsync(policy, items, timeRange, dimQuery).map { seq =>
      for {
        FetchedCountsGrouped(k, intervalWithCountMap) <- seq
      } yield {
        DecayedCounts(k, {
          for {
            ((interval, dimKeyValues), grouped) <- intervalWithCountMap
          } yield {
            val (tqFrom, tqTo) = groupedTimeRange(interval).head
            val formula = {
              for {
                strSum <- qsSum
                (func, arg) <- FunctionParser(strSum)
              } yield {
                // apply function
                func.toLowerCase match {
                  case "exp_decay" => ExpDecayFormula.byMeanLifeTime(arg.toLong * TimedQualifier.getTsUnit(interval))
                  case _ => throw new UnsupportedOperationException(s"unknown function: $strSum")
                }
              }
            }
            ExactQualifier(tqFrom, dimKeyValues) -> {
              grouped.map { case (eq, count) =>
                formula match {
                  case Some(decay) =>
                    decay(count, tqTo.ts - eq.tq.ts)
                  case None =>
                    count
                }
              }.sum
            }
          }
        })
      }
    }
  }

  def updateCount(policy: Counter, counts: Seq[(ExactKeyTrait, ExactValueMap)]): Seq[TrxLog] = {
    ready(policy) match {
      case true =>
        val updateResults = storage.update(policy, counts)
        for {
          (exactKey, values) <- counts
          results = updateResults.getOrElse(exactKey, Nil.toMap)
        } yield {
          TrxLog(results.nonEmpty, exactKey.policyId, exactKey.itemKey, makeTrxLogResult(values, results))
        }
      case false =>
        Nil
    }
  }

  def deleteCount(policy: Counter, keys: Seq[ExactKeyTrait]): Unit = {
    storage.delete(policy, keys)
  }

  private def makeTrxLogResult(values: ExactValueMap, results: ExactValueMap): Seq[TrxLogResult] = {
    for {
      (eq, value) <- values
    } yield {
      val result = results.getOrElse(eq, -1l)
      TrxLogResult(eq.tq.q.toString, eq.tq.ts, eq.dimension, value, result)
    }
  }.toSeq

  def insertBlobValue(policy: Counter, keys: Seq[BlobExactKey]): Seq[Boolean] = {
    storage.insertBlobValue(policy, keys)
  }

  def getBlobValue(policy: Counter, blobId: String): Option[String] = {
    storage.getBlobValue(policy, blobId)
  }

  def prepare(policy: Counter) = {
    storage.prepare(policy)
  }

  def destroy(policy: Counter) = {
    storage.destroy(policy)
  }

  def ready(policy: Counter): Boolean = {
    storageStatusCache.withCache(s"${policy.id}") {
      val ready = storage.ready(policy)
      if (!ready) {
        // if key is not in cache, log message
        log.warn(s"${policy.service}.${policy.action} storage is not ready.")
      }
      Some(ready)
    }.getOrElse(false)
  }
}

object ExactCounter {
  object ColumnFamily extends Enumeration {
    type ColumnFamily = Value

    val SHORT = Value("s")
    val LONG = Value("l")
  }
  import IntervalUnit._
  val intervalsMap = Map(MINUTELY -> ColumnFamily.SHORT, HOURLY -> ColumnFamily.SHORT,
    DAILY -> ColumnFamily.LONG, MONTHLY -> ColumnFamily.LONG, TOTAL -> ColumnFamily.LONG)

  val blobCF = ColumnFamily.LONG.toString.getBytes
  val blobColumn = "b".getBytes

  type ExactValueMap = Map[ExactQualifier, Long]
}
