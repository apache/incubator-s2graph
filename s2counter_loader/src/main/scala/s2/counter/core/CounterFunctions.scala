package s2.counter.core

import com.kakao.s2graph.core.GraphUtil
import kafka.producer.KeyedMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, Logging}
import play.api.libs.json.{JsString, JsNumber, JsValue, Json}
import s2.config.{S2ConfigFactory, StreamingConfig}
import s2.counter.TrxLog
import s2.counter.core.ExactCounter.ExactValueMap
import s2.counter.core.RankingCounter.RankingValueMap
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core.v2.{ExactStorageGraph, RankingStorageGraph}
import s2.models.{Counter, DBModel, DefaultCounterModel}
import s2.spark.WithKafka

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.language.postfixOps
import scala.util.Try

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 10. 6..
 */
object CounterFunctions extends Logging with WithKafka {
  import scala.concurrent.ExecutionContext.Implicits.global

  private val K_MAX = 500

  val exactCounter = new ExactCounter(S2ConfigFactory.config, new ExactStorageGraph(S2ConfigFactory.config))
  val rankingCounter = new RankingCounter(S2ConfigFactory.config, new RankingStorageGraph(S2ConfigFactory.config))

  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  type HashMapAccumulable = Accumulable[MutableHashMap[String, Long], (String, Long)]

  val initialize = {
    logInfo("initialize CounterFunctions")
    DBModel.initialize(S2ConfigFactory.config)
    true
  }

  def getCountValue(policy: Counter, item: CounterEtlItem): ExactValueMap = {
    for {
      dimKeys <- policy.dimensionList
      dimValues <- getDimensionValues(item.dimension, dimKeys).toSeq
      eq <- ExactQualifier.getQualifiers(policy.intervals.map(IntervalUnit.withName), item.ts, dimKeys.zip(dimValues).toMap)
    } yield {
      eq -> item.value
    }
  }.toMap

  def getDimensionValues(dimension: JsValue, keys: Array[String]): Option[Array[String]] = {
    Try {
      for {
        k <- keys
        jsValue = dimension \ k
      } yield {
        jsValue match {
          case JsNumber(n) => n.toString()
          case JsString(s) => s
          case _ => throw new Exception()
        }
      }
    }.toOption
  }
  
  def exactMapper(item: CounterEtlItem): Option[(ExactKeyTrait, ExactValueMap)] = {
    DefaultCounterModel.findByServiceAction(item.service, item.action).map { policy =>
      (ExactKey(policy, item.item, checkItemType = true), getCountValue(policy, item))
    }
  }

  def rankingMapper(row: ItemRankingRow): Seq[(RankingKey, RankingValueMap)] = {
    // normal ranking
    for {
      (eq, rv) <- row.value
    } yield {
      (RankingKey(row.key.policyId, row.key.version, eq), Map(row.key.itemKey -> rv))
    }
  }.toSeq

  def logToRankValue(log: TrxLog): Option[(ExactKeyTrait, Map[ExactQualifier, RankingValue])] = {
    DefaultCounterModel.findById(log.policyId).map { policy =>
      val key = ExactKey(policy, log.item, checkItemType = false)
      val value = {
        for {
          result <- log.results
        } yield {
          ExactQualifier(TimedQualifier(result.interval, result.ts), result.dimension) -> RankingValue(result.result, result.value)
        }
      }.toMap
      key -> value
    }
  }

  def reduceValue[T, U](op: (U, U) => U, default: U)(m1: Map[T, U], m2: Map[T, U]): Map[T, U] = {
    m1 ++ m2.map { case (k, v) =>
      k -> op(m1.getOrElse(k, default), v)
    }
  }

  def makeExactRdd(rdd: RDD[(String, String)], numPartitions: Int): RDD[(ExactKeyTrait, ExactValueMap)] = {
    rdd.mapPartitions { part =>
      assert(initialize)
      for {
        (k, v) <- part
        line <- GraphUtil.parseString(v)
        item <- CounterEtlItem(line).toSeq
        ev <- exactMapper(item).toSeq
      } yield {
        ev
      }
    }.reduceByKey(reduceValue[ExactQualifier, Long](_ + _, 0L)(_, _), numPartitions)
  }

  def makeRankingRdd(rdd: RDD[(String, String)], numPartitions: Int): RDD[(RankingKey, RankingValueMap)] = {
    val logRdd = makeTrxLogRdd(rdd, numPartitions)
    makeRankingRddFromTrxLog(logRdd, numPartitions)
  }

  def makeRankingRddFromTrxLog(rdd: RDD[TrxLog], numPartitions: Int): RDD[(RankingKey, RankingValueMap)] = {
    val itemRankingRdd = makeItemRankingRdd(rdd, numPartitions).cache()
    try {
      rankingCount(itemRankingRdd, numPartitions) union
        rateRankingCount(itemRankingRdd, numPartitions) union
        trendRankingCount(itemRankingRdd, numPartitions) coalesce numPartitions
    } finally {
      itemRankingRdd.unpersist(false)
    }
  }
  
  def makeTrxLogRdd(rdd: RDD[(String, String)], numPartitions: Int): RDD[TrxLog] = {
    rdd.mapPartitions { part =>
      assert(initialize)
      for {
        (k, v) <- part
        line <- GraphUtil.parseString(v)
        trxLog = Json.parse(line).as[TrxLog] if trxLog.success
      } yield {
        trxLog
      }
    }
  }

  def rankingCount(rdd: RDD[ItemRankingRow], numPartitions: Int): RDD[(RankingKey, RankingValueMap)] = {
    rdd.mapPartitions { part =>
      for {
        row <- part
        rv <- rankingMapper(row)
      } yield {
        rv
      }
    }.reduceByKey(reduceValue(RankingValue.reduce, RankingValue(0, 0))(_, _), numPartitions)
  }

  case class ItemRankingRow(key: ExactKeyTrait, value: Map[ExactQualifier, RankingValue])

  def makeItemRankingRdd(rdd: RDD[TrxLog], numPartitions: Int): RDD[ItemRankingRow] = {
    rdd.mapPartitions { part =>
      for {
        log <- part
        rv <- logToRankValue(log)
      } yield {
        rv
      }
    }.reduceByKey(reduceValue(RankingValue.reduce, RankingValue(0, 0))(_, _), numPartitions).mapPartitions { part =>
      for {
        (key, value) <- part
      } yield {
        ItemRankingRow(key, value)
      }
    }
  }

  def mapTrendRankingValue(rows: Seq[ItemRankingRow]): Seq[(ExactKeyTrait, Map[ExactQualifier, RateRankingValue])] = {
    for {
      row <- rows
      trendPolicy <- DefaultCounterModel.findByTrendActionId(row.key.policyId)
    } yield {
      val key = ExactKey(trendPolicy, row.key.itemKey, checkItemType = false)
      val value = row.value.filter { case (eq, rv) =>
        // eq filter by rate policy dimension
        trendPolicy.dimensionSet.exists { dimSet => dimSet == eq.dimKeyValues.keys }
      }.map { case (eq, rv) =>
        eq -> RateRankingValue(rv.score, -1)
      }
      (key, value)
    }
  }

  def mapRateRankingValue(rows: Seq[ItemRankingRow]): Seq[(ExactKeyTrait, Map[ExactQualifier, RateRankingValue])] = {
    val actionPart = {
      for {
        row <- rows
        ratePolicy <- DefaultCounterModel.findByRateActionId(row.key.policyId)
      } yield {
        val key = ExactKey(ratePolicy, row.key.itemKey, checkItemType = false)
        val value = row.value.filter { case (eq, rv) =>
          // eq filter by rate policy dimension
          ratePolicy.dimensionSet.exists { dimSet => dimSet == eq.dimKeyValues.keys }
        }.map { case (eq, rv) =>
          eq -> RateRankingValue(rv.score, -1)
        }
        (key, value)
      }
    }

    val basePart = {
      for {
        row <- rows
        ratePolicy <- DefaultCounterModel.findByRateBaseId(row.key.policyId)
      } yield {
        val key = ExactKey(ratePolicy, row.key.itemKey, checkItemType = false)
        val value = row.value.filter { case (eq, rv) =>
          // eq filter by rate policy dimension
          ratePolicy.dimensionSet.exists { dimSet => dimSet == eq.dimKeyValues.keys }
        }.map { case (eq, rv) =>
          eq -> RateRankingValue(-1, rv.score)
        }
        (key, value)
      }
    }

    actionPart ++ basePart
  }

  def trendRankingCount(rdd: RDD[ItemRankingRow], numPartitions: Int): RDD[(RankingKey, RankingValueMap)] = {
    rdd.mapPartitions { part =>
      mapTrendRankingValue(part.toSeq) toIterator
    }.reduceByKey(reduceValue(RateRankingValue.reduce, RateRankingValue(-1, -1))(_, _), numPartitions).mapPartitions { part =>
      val missingByPolicy = {
        for {
          (key, value) <- part.toSeq
          trendPolicy <- DefaultCounterModel.findById(key.policyId).toSeq
          actionId <- trendPolicy.rateActionId.toSeq
          actionPolicy <- DefaultCounterModel.findById(actionId).toSeq
        } yield {
          // filter total eq
          val missingQualifiersWithRRV = value.filterKeys { eq => eq.tq.q != IntervalUnit.TOTAL }
          (actionPolicy, key, missingQualifiersWithRRV)
        }
      }.groupBy(_._1).mapValues(seq => seq.map(x => (x._2, x._3)))

      val filled = {
        for {
          (policy, missing) <- missingByPolicy.toSeq
          keyWithPast = exactCounter.getPastCounts(policy, missing.map { case (k, v) => k.itemKey -> v.keys.toSeq })
          (key, current) <- missing
        } yield {
          val past = keyWithPast.getOrElse(key.itemKey, Map.empty[ExactQualifier, Long])
          val base = past.mapValues(l => RateRankingValue(-1, l))
//          log.warn(s"trend: $policy $key -> $current $base")
          key -> reduceValue(RateRankingValue.reduce, RateRankingValue(-1, -1))(current, base)
        }
      }

//      filled.foreach(println)

      {
        // filter by rate threshold
        for {
          (key, value) <- filled
          ratePolicy <- DefaultCounterModel.findById(key.policyId).toSeq
          (eq, rrv) <- value if rrv.baseScore >= ratePolicy.rateThreshold.getOrElse(Int.MinValue)
        } yield {
          (RankingKey(key.policyId, key.version, eq), Map(key.itemKey -> rrv.rankingValue))
        }
      } toIterator
    }.reduceByKey(reduceValue(RankingValue.reduce, RankingValue(0, 0))(_, _), numPartitions)
  }

  def rateRankingCount(rdd: RDD[ItemRankingRow], numPartitions: Int): RDD[(RankingKey, RankingValueMap)] = {
    rdd.mapPartitions { part =>
      mapRateRankingValue(part.toSeq) toIterator
    }.reduceByKey(reduceValue(RateRankingValue.reduce, RateRankingValue(-1, -1))(_, _), numPartitions).mapPartitions { part =>
      val seq = part.toSeq
//      seq.foreach(x => println(s"item ranking row>> $x"))

      // find and evaluate action value is -1
      val actionMissingByPolicy = {
        for {
          (key, value) <- seq if value.exists { case (eq, rrv) => rrv.actionScore == -1 }
          ratePolicy <- DefaultCounterModel.findById(key.policyId).toSeq
          actionId <- ratePolicy.rateActionId.toSeq
          actionPolicy <- DefaultCounterModel.findById(actionId)
        } yield {
          (actionPolicy, key, value.filter { case (eq, rrv) => rrv.actionScore == -1 })
        }
      }.groupBy(_._1).mapValues(seq => seq.map(x => (x._2, x._3)))

      val actionFilled = {
        for {
          (actionPolicy, actionMissing) <- actionMissingByPolicy.toSeq
          keyWithRelated = exactCounter.getRelatedCounts(actionPolicy, actionMissing.map { case (k, v) => k.itemKey -> v.keys.toSeq })
          (key, current) <- actionMissing
        } yield {
          val related = keyWithRelated.getOrElse(key.itemKey, Map.empty[ExactQualifier, Long])
          val found = related.mapValues(l => RateRankingValue(l, -1))
          val filled = reduceValue(RateRankingValue.reduce, RateRankingValue(-1, -1))(current, found)
//          log.warn(s"action: $key -> $found $filled")
          key -> filled
        }
      }

//      actionFilled.foreach(x => println(s"action filled>> $x"))

      // find and evaluate base value is -1
      val baseMissingByPolicy = {
        for {
          (key, value) <- seq if value.exists { case (eq, rrv) => rrv.baseScore == -1 }
          ratePolicy <- DefaultCounterModel.findById(key.policyId).toSeq
          baseId <- ratePolicy.rateBaseId.toSeq
          basePolicy <- DefaultCounterModel.findById(baseId)
        } yield {
          (basePolicy, key, value.filter { case (eq, rrv) => rrv.baseScore == -1 })
        }
      }.groupBy(_._1).mapValues(seq => seq.map(x => (x._2, x._3)))

      val baseFilled = {
        for {
          (basePolicy, baseMissing) <- baseMissingByPolicy.toSeq
          keyWithRelated = exactCounter.getRelatedCounts(basePolicy, baseMissing.map { case (k, v) => k.itemKey -> v.keys.toSeq })
          (key, current) <- baseMissing
        } yield {
          val related = keyWithRelated.getOrElse(key.itemKey, Map.empty[ExactQualifier, Long])
          val found = related.mapValues(l => RateRankingValue(-1, l))
          val filled = reduceValue(RateRankingValue.reduce, RateRankingValue(-1, -1))(current, found)
//          log.warn(s"base: $basePolicy $key -> $found $filled")
          key -> filled
        }
      }

//      baseFilled.foreach(x => println(s"base filled>> $x"))

      val alreadyFilled = {
        for {
          (key, value) <- seq if value.exists { case (eq, rrv) => rrv.actionScore != -1 && rrv.baseScore != -1 }
        } yield {
          key -> value.filter { case (eq, rrv) => rrv.actionScore != -1 && rrv.baseScore != -1 }
        }
      }

      val rtn = {
        // filter by rate threshold
        for {
          (key, value) <- actionFilled ++ baseFilled ++ alreadyFilled
          ratePolicy <- DefaultCounterModel.findById(key.policyId).toSeq
          (eq, rrv) <- value if rrv.baseScore >= ratePolicy.rateThreshold.getOrElse(Int.MinValue)
        } yield {
          (RankingKey(key.policyId, key.version, eq), Map(key.itemKey -> rrv.rankingValue))
        }
      }
      rtn.toIterator
    }.reduceByKey(reduceValue(RankingValue.reduce, RankingValue(0, 0))(_, _), numPartitions)
  }

  def insertBlobValue(keys: Seq[BlobExactKey], acc: HashMapAccumulable): Unit = {
    val keyByPolicy = {
      for {
        key <- keys
        policy <- DefaultCounterModel.findById(key.policyId)
      } yield {
        (policy, key)
      }
    }.groupBy(_._1).mapValues(values => values.map(_._2))

    for {
      (policy, allKeys) <- keyByPolicy
      keys <- allKeys.grouped(10)
      success <- exactCounter.insertBlobValue(policy, keys)
    } yield {
      success match {
        case true => acc += ("BLOB", 1)
        case false => acc += ("BLOBFailed", 1)
      }
    }
  }

  def updateExactCounter(counts: Seq[(ExactKeyTrait, ExactValueMap)], acc: HashMapAccumulable): Seq[TrxLog] = {
    val countsByPolicy = {
      for {
        (key, count) <- counts
        policy <- DefaultCounterModel.findById(key.policyId)
      } yield {
        (policy, (key, count))
      }
    }.groupBy { case (policy, _) => policy }.mapValues(values => values.map(_._2))

    for {
      (policy, allCounts) <- countsByPolicy
      counts <- allCounts.grouped(10)
      trxLog <- exactCounter.updateCount(policy, counts)
    } yield {
      trxLog.success match {
        case true => acc += (s"ExactV${policy.version}", 1)
        case false => acc += (s"ExactFailedV${policy.version}", 1)
      }
      trxLog
    }
  }.toSeq

  def exactCountFromEtl(rdd: RDD[CounterEtlItem], numPartitions: Int): RDD[(ExactKeyTrait, ExactValueMap)] = {
    rdd.mapPartitions { part =>
      for {
        item <- part
        ev <- exactMapper(item).toSeq
      } yield {
        ev
      }
    }.reduceByKey(reduceValue[ExactQualifier, Long](_ + _, 0L)(_, _), numPartitions)
  }

  def updateRankingCounter(values: TraversableOnce[(RankingKey, RankingValueMap)], acc: HashMapAccumulable): Unit = {
    assert(initialize)
    val valuesByPolicy = {
      for {
        (key, value) <- values.toSeq
        policy <- DefaultCounterModel.findById(key.policyId)
        if policy.useRank && rankingCounter.ready(policy) // update only rank counter enabled and ready
      } yield {
        (policy, (key, value))
      }
    }.groupBy { case (policy, _) => policy }.mapValues(values => values.map(_._2))

    for {
      (policy, allValues) <- valuesByPolicy
      groupedValues <- allValues.grouped(10)
    } {
      rankingCounter.update(groupedValues, K_MAX)
      acc += (s"RankingV${policy.version}", groupedValues.length)
    }
  }
  
  def produceTrxLog(trxLogs: TraversableOnce[TrxLog]): Unit = {
    for {
      trxLog <- trxLogs
    } {
      val topic = trxLog.success match {
        case true => StreamingConfig.KAFKA_TOPIC_COUNTER_TRX
        case false => StreamingConfig.KAFKA_TOPIC_COUNTER_FAIL
      }
      val msg = new KeyedMessage[String, String](topic, s"${trxLog.policyId}${trxLog.item}", Json.toJson(trxLog).toString())
      producer.send(msg)
    }
  }
}
