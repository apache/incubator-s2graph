package s2.counter.core

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core.TimedQualifier.IntervalUnit.IntervalUnit
import s2.counter.decay.ExpDecayFormula
import s2.counter.{TrxLog, TrxLogResult}
import s2.models.Counter
import s2.util.FunctionParser

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
  
  /**
   * dimension: age, value of ages
   * TODO: filter dimension in core.
   */
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
    val updateResults = storage.update(policy, counts)
    for {
      (exactKey, values) <- counts
      results = updateResults.getOrElse(exactKey, Nil.toMap)
    } yield {
      TrxLog(results.nonEmpty, exactKey.policyId, exactKey.itemKey, makeTrxLogResult(values, results))
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
//
//  def getStartRowKeyPrefix(policy: Counter): Array[Byte] = {
//    bytesUtil.getRowKeyPrefix(policy.id)
//  }
//
//  def getStopRowKeyPrefix(policy: Counter): Array[Byte] = {
//    bytesUtil.getRowKeyPrefix(policy.id + 1)
//  }
//
//  def fetchItems(policy: Counter, limit: Int, cursor: Option[String] = None): Seq[Array[Byte]] = {
//    //    policy.version match {
//    //      case 0x01 =>
//    //        throw MethodNotSupportedException(s"fetchItems operation not supported in version(1)")
//    //    }
//
//    val startKey = cursor.map { s =>
//      // to exclude start row, add a trailing 0 byte
//      Base64.decode(s) ++ Array.fill[Byte](1)(0)
//    }.getOrElse(Array.empty[Byte])
//
//    val scan = new Scan(getStartRowKeyPrefix(policy) ++ startKey, getStopRowKeyPrefix(policy))
//
//    withHBase(getTableName(policy)) { table =>
//      val scanner = new DistributedScanner(table, scan)
//      scanner.next(limit).map { result =>
//        DistributedScanner.getRealRowKey(result)
//      }
//    } match {
//      case Success(rst) =>
//        rst.toSeq
//      case Failure(ex) =>
//        log.error(s"$ex")
//        Seq.empty[Array[Byte]]
//    }
//  }

  def prepare(policy: Counter) = {
    storage.prepare(policy)
  }

  def destroy(policy: Counter) = {
    storage.destroy(policy)
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
