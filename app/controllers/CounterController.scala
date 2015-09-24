package controllers

import com.daumkakao.s2graph.core.ExceptionHandler
import com.daumkakao.s2graph.core.ExceptionHandler.KafkaMessage
import com.daumkakao.s2graph.core.mysqls.Label
import config.CounterConfig
import models._
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.Play
import play.api.libs.json._
import play.api.mvc.{Action, Controller, Request}
import s2.config.S2CounterConfig
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core._
import s2.counter.core.v1.{ExactStorageHBaseV1, RankingStorageV1}
import s2.counter.core.v2.{ExactStorageGraph, RankingStorageGraph}
import s2.models.Counter.ItemType
import s2.models.{Counter, CounterModel}
import s2.util.{CartesianProduct, ReduceMapValue, UnitConverter}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 22..
 */
object CounterController extends Controller {
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  val config = Play.current.configuration.underlying
  val s2config = new S2CounterConfig(config)

  private val exactCounterMap = Map(
    s2.counter.VERSION_1 -> new ExactCounter(config, new ExactStorageHBaseV1(config)),
    s2.counter.VERSION_2 -> new ExactCounter(config, new ExactStorageGraph(config))
  )
  private val rankingCounterMap = Map(
    s2.counter.VERSION_1 -> new RankingCounter(config, new RankingStorageV1(config)),
    s2.counter.VERSION_2 -> new RankingCounter(config, new RankingStorageGraph(config))
  )

  private def exactCounter(version: Byte): ExactCounter = exactCounterMap(version)
  private def rankingCounter(version: Byte): RankingCounter = rankingCounterMap(version)

  lazy val counterModel = new CounterModel(config)

  def getQueryString[T](key: String, default: String)(implicit request: Request[T]): String = {
    request.getQueryString(key).getOrElse(default)
  }

  def createAction(service: String, action: String) = Action(s2parse.json) { implicit request =>
    counterModel.findByServiceAction(service, action, useCache = false) match {
      case None =>
        val body = request.body
        val version = (body \ "version").asOpt[Int].map(_.toByte).getOrElse(s2.counter.VERSION_2)
        val autoComb = (body \ "autoComb").asOpt[Boolean].getOrElse(true)
        val dimension = (body \ "dimension").asOpt[String].getOrElse("")
        val useProfile = (body \ "useProfile").asOpt[Boolean].getOrElse(false)
        val useExact = (body \ "useExact").asOpt[Boolean].getOrElse(true)
        val useRank = (body \ "useRank").asOpt[Boolean].getOrElse(true)

        val intervalUnit = (body \ "intervalUnit").asOpt[String]
        // 2 day
        val ttl = (body \ "ttl").asOpt[Int].getOrElse(2 * 24 * 60 * 60)
        val dailyTtl = (body \ "dailyTtl").asOpt[Int]
        val regionMultiplier = (body \ "regionMultiplier").asOpt[Int].getOrElse(1)

        val rateAction = (body \ "rateAction").asOpt[Map[String, String]]
        val rateBase = (body \ "rateBase").asOpt[Map[String, String]]
        val rateThreshold = (body \ "rateThreshold").asOpt[Int]

        val rateActionId = {
          for {
            actionMap <- rateAction
            service <- actionMap.get("service")
            action <- actionMap.get("action")
            policy <- counterModel.findByServiceAction(service, action)
          } yield {
            policy.id
          }
        }
        val rateBaseId = {
          for {
            actionMap <- rateBase
            service <- actionMap.get("service")
            action <- actionMap.get("action")
            policy <- counterModel.findByServiceAction(service, action)
          } yield {
            policy.id
          }
        }

        val hbaseTable = {
          Seq("s2counter_v2", service, ttl) ++ dailyTtl mkString "_"
        }

        // find label
        val itemType = Label.findByName(action, useCache = false) match {
          case Some(label) =>
            ItemType.withName(label.tgtColumnType.toUpperCase)
          case None =>
            val strItemType = (body \ "itemType").asOpt[String].getOrElse("STRING")
            ItemType.withName(strItemType.toUpperCase)
        }
        val policy = Counter(useFlag = true, version, service, action, itemType, autoComb = autoComb, dimension,
          useProfile = useProfile, useRank = useRank, ttl, dailyTtl, Some(hbaseTable), intervalUnit,
          rateActionId, rateBaseId, rateThreshold)

        // prepare exact storage
        exactCounter(version).prepare(policy)
        if (useRank) {
          // prepare ranking storage
          rankingCounter(version).prepare(policy, rateAction.flatMap(_.get("action")))
        }
        counterModel.createServiceAction(policy)
        Ok(Json.toJson(Map("msg" -> s"created $service/$action")))
      case Some(policy) =>
        Ok(Json.toJson(Map("msg" -> s"already exist $service/$action")))
    }
  }

  def getAction(service: String, action: String) = Action { request =>
    Ok("")
  }

  def updateAction(service: String, action: String) = Action(s2parse.json) { request =>
    Ok("")
  }

  def deleteAction(service: String, action: String) = Action.apply {
    {
      for {
        policy <- counterModel.findByServiceAction(service, action)
      } yield {
        Try {
          exactCounter(policy.version).destroy(policy)
          if (policy.useRank) {
            rankingCounter(policy.version).destroy(policy)
          }
          counterModel.deleteServiceAction(policy)
        } match {
          case Success(v) =>
            Ok(Json.toJson(Map("msg" -> s"deleted $service/$action")))
          case Failure(ex) =>
            throw ex
        }
      }
    }.getOrElse(NotFound(Json.toJson(Map("msg" -> s"$service.$action not found"))))
  }

  def getExactCountAsync(service: String, action: String, item: String) = Action.async { implicit request =>
    val intervalUnits = getQueryString("interval", getQueryString("step", "t")).split(',').toSeq
      .map(IntervalUnit.withName)
    val limit = getQueryString("limit", "1").toInt

    val qsSum = request.getQueryString("sum")

    val optFrom = request.getQueryString("from").filter(!_.isEmpty).map(UnitConverter.toMillis)
    val optTo = request.getQueryString("to").filter(!_.isEmpty).map(UnitConverter.toMillis)

    val limitOpt = (optFrom, optTo) match {
      case (Some(_), Some(_)) =>
        None
      case _ =>
        Some(limit)
    }

    // find dimension
    lazy val dimQueryValues = request.queryString.filterKeys { k => k.charAt(0) == ':' }.map { case (k, v) =>
      (k.substring(1), v.mkString(",").split(',').filter(_.nonEmpty).toSet)
    }
//    Logger.warn(s"$dimQueryValues")

    counterModel.findByServiceAction(service, action) match {
      case Some(policy) =>
        val tqs = TimedQualifier.getQualifiersToLimit(intervalUnits, limit, optFrom, optTo)
        val timeRange = TimedQualifier.getTimeRange(intervalUnits, limit, optFrom, optTo)
        try {
//          Logger.warn(s"$tqs $qsSum")
          if (tqs.head.length > 1 && qsSum.isDefined) {
            getDecayedCountToJs(policy, item, timeRange, dimQueryValues, qsSum).map { jsVal =>
              Ok(jsVal)
            }
          } else {
            getExactCountToJs(policy, item, timeRange, limitOpt, dimQueryValues).map { jsVal =>
              Ok(jsVal)
            }
          }
        } catch {
          case e: Exception =>
            throw e
//            Future.successful(BadRequest(s"$service, $action, $item"))
        }
      case None =>
        Future.successful(NotFound(Json.toJson(Map("msg" -> s"$service.$action not found"))))
    }
  }

  /**
   * [{
   *    "service": , "action", "itemIds": [], "interval": string, "limit": int, "from": ts, "to": ts,
   *    "dimensions": [{"key": list[String]}]
   * }]
   * @return
   */
  private def parseExactCountParam(jsValue: JsValue) = {
    val service = (jsValue \ "service").as[String]
    val action = (jsValue \ "action").as[String]
    val itemIds = (jsValue \ "itemIds").as[Seq[String]]
    val intervals = (jsValue \ "intervals").asOpt[Seq[String]].getOrElse(Seq("t")).distinct.map(IntervalUnit.withName)
    val limit = (jsValue \ "limit").asOpt[Int].getOrElse(1)
    val from = (jsValue \ "from").asOpt[Long]
    val to = (jsValue \ "to").asOpt[Long]
    val sum = (jsValue \ "sum").asOpt[String]
    val dimensions = {
      for {
        dimension <- (jsValue \ "dimensions").asOpt[Seq[JsObject]].getOrElse(Nil)
        (k, vs) <- dimension.fields
      } yield {
        k -> vs.as[Seq[String]].toSet
      }
    }.toMap
    (service, action, itemIds, intervals, limit, from, to, dimensions, sum)
  }

  def getExactCountAsyncMulti = Action.async(s2parse.json) { implicit request =>
    val jsValue = request.body
    try {
      val futures = {
        for {
          jsObject <- jsValue.asOpt[List[JsObject]].getOrElse(Nil)
          (service, action, itemIds, intervalUnits, limit, from, to, dimQueryValues, qsSum) = parseExactCountParam(jsObject)
          optFrom = from.map(UnitConverter.toMillis)
          optTo = to.map(UnitConverter.toMillis)
          timeRange = TimedQualifier.getTimeRange(intervalUnits, limit, optFrom, optTo)
          policy <- counterModel.findByServiceAction(service, action).toSeq
          item <- itemIds
        } yield {
          val tqs = TimedQualifier.getQualifiersToLimit(intervalUnits, limit, optFrom, optTo)
          val timeRange = TimedQualifier.getTimeRange(intervalUnits, limit, optFrom, optTo)
          val limitOpt = (optFrom, optTo) match {
            case (Some(_), Some(_)) =>
              None
            case _ =>
              Some(limit)
          }

//          Logger.warn(s"$item, $limit, $optFrom, $optTo, $qsSum, $tqs")

          if (tqs.head.length > 1 && qsSum.isDefined) {
            getDecayedCountToJs(policy, item, timeRange, dimQueryValues, qsSum)
          } else {
            getExactCountToJs(policy, item, timeRange, limitOpt, dimQueryValues)
          }
        }
      }
      Future.sequence(futures).map { rets =>
        Ok(Json.toJson(rets))
      }
    } catch {
      case e: Exception =>
        throw e
//        Future.successful(BadRequest(s"$jsValue"))
    }
  }

  private [controllers] def fetchedToResult(fetchedCounts: FetchedCountsGrouped, limitOpt: Option[Int]): Seq[ExactCounterIntervalItem] = {
    for {
      ((interval, dimKeyValues), values) <- fetchedCounts.intervalWithCountMap
    } yield {
      val counterItems = {
        val sortedItems = values.toSeq.sortBy { case (eq, v) => -eq.tq.ts }
        val limited = limitOpt match {
          case Some(limit) => sortedItems.take(limit)
          case None => sortedItems
        }
        for {
          (eq, value) <- limited
        } yield {
          ExactCounterItem(eq.tq.ts, value, value.toDouble)
        }
      }
      ExactCounterIntervalItem(interval.toString, dimKeyValues, counterItems)
    }
  }.toSeq

  private def decayedToResult(decayedCounts: DecayedCounts): Seq[ExactCounterIntervalItem] = {
    for {
      (eq, score) <- decayedCounts.qualifierWithCountMap
    } yield {
      ExactCounterIntervalItem(eq.tq.q.toString, eq.dimKeyValues, Seq(ExactCounterItem(eq.tq.ts, score.toLong, score)))
    }
  }.toSeq

  private def getExactCountToJs(policy: Counter,
                                item: String,
                                timeRange: Seq[(TimedQualifier, TimedQualifier)],
                                limitOpt: Option[Int],
                                dimQueryValues: Map[String, Set[String]]): Future[JsValue] = {
    exactCounter(policy.version).getCountsAsync(policy, Seq(item), timeRange, dimQueryValues).map { seq =>
      val items = {
        for {
          fetched <- seq
        } yield {
          fetchedToResult(fetched, limitOpt)
        }
      }.flatten
      Json.toJson(ExactCounterResult(ExactCounterResultMeta(policy.service, policy.action, item), items))
    }
  }

  private def getDecayedCountToJs(policy: Counter,
                                  item: String,
                                  timeRange: Seq[(TimedQualifier, TimedQualifier)],
                                  dimQueryValues: Map[String, Set[String]],
                                  qsSum: Option[String]): Future[JsValue] = {
    exactCounter(policy.version).getDecayedCountsAsync(policy, Seq(item), timeRange, dimQueryValues, qsSum).map { seq =>
      val decayedCounts = seq.head
      val meta = ExactCounterResultMeta(policy.service, policy.action, decayedCounts.exactKey.itemKey)
      val intervalItems = decayedToResult(decayedCounts)
      Json.toJson(ExactCounterResult(meta, intervalItems))
    }
  }

  def getRankingCountAsync(service: String, action: String) = Action.async { implicit request =>
    lazy val intervalUnits = getQueryString("interval", getQueryString("step", "t")).split(',').toSeq
      .map(IntervalUnit.withName)
    lazy val limit = getQueryString("limit", "1").toInt
    lazy val kValue = getQueryString("k", "10").toInt

    lazy val qsSum = request.getQueryString("sum")

    lazy val optFrom = request.getQueryString("from").filter(!_.isEmpty).map(UnitConverter.toMillis)
    lazy val optTo = request.getQueryString("to").filter(!_.isEmpty).map(UnitConverter.toMillis)

    // find dimension
    lazy val dimensionMap = request.queryString.filterKeys { k => k.charAt(0) == ':' }.map { case (k, v) =>
      (k.substring(1), v.mkString(",").split(',').toList)
    }

    val dimensions = {
      for {
        values <- CartesianProduct(dimensionMap.values.toList).toSeq
      } yield {
        dimensionMap.keys.zip(values).toMap
      }
    }

    counterModel.findByServiceAction(service, action) match {
      case Some(policy) =>
        val tqs = TimedQualifier.getQualifiersToLimit(intervalUnits, limit, optTo)
        val dimKeys = {
          for {
            dimension <- dimensions
          } yield {
            dimension -> tqs.map(tq => RankingKey(policy.id, policy.version, ExactQualifier(tq, dimension)))
          }
        }

        // if tqs has only 1 tq, do not apply sum function
        try {
          val rankResult = {
            if (tqs.length > 1 && qsSum.isDefined) {
              getSumRankCounterResultAsync(policy, dimKeys, kValue, qsSum)
            } else {
              // no summary
              Future.successful(getRankCounterResult(policy, dimKeys, kValue))
            }
          }

          rankResult.map { result =>
            Ok(Json.toJson(result))
          }
        } catch {
          case e: UnsupportedOperationException =>
            Future.successful(NotImplemented(Json.toJson(
              Map("msg" -> e.getMessage)
            )))
          case e: Throwable =>
            throw e
        }
      case None =>
        Future.successful(NotFound(Json.toJson(Map("msg" -> s"$service.$action not found"))))
    }
  }

  def deleteRankingCount(service: String, action: String) = Action.async { implicit request =>
    lazy val intervalUnits = getQueryString("interval", getQueryString("step", "t")).split(',').toSeq
      .map(IntervalUnit.withName)
    lazy val limit = getQueryString("limit", "1").toInt

    // find dimension
    lazy val dimensionMap = request.queryString.filterKeys { k => k.charAt(0) == ':' }.map { case (k, v) =>
      (k.substring(1), v.mkString(",").split(',').toList)
    }

    val dimensions = {
      for {
        values <- CartesianProduct(dimensionMap.values.toList).toSeq
      } yield {
        dimensionMap.keys.zip(values).toMap
      }
    }

    Future {
      counterModel.findByServiceAction(service, action) match {
        case Some(policy) =>
          val tqs = TimedQualifier.getQualifiersToLimit(intervalUnits, limit)
          val keys = {
            for {
              dimension <- dimensions
              tq <- tqs
            } yield {
              RankingKey(policy.id, policy.version, ExactQualifier(tq, dimension))
            }
          }

          for {
            key <- keys
          } {
            rankingCounter(policy.version).delete(key)
          }

          Ok(JsObject(
            Seq(
              ("msg", Json.toJson(s"delete ranking in $service.$action")),
              ("items", Json.toJson({
                for {
                  key <- keys
                } yield {
                  s"${key.eq.tq.q}.${key.eq.tq.ts}.${key.eq.dimension}"
                }
              }))
            )
          ))
        case None =>
          NotFound(Json.toJson(
            Map("msg" -> s"$service.$action not found")
          ))
      }
    }
  }

  val reduceRateRankingValue = new ReduceMapValue[ExactKeyTrait, RateRankingValue](RateRankingValue.reduce, RateRankingValue(-1, -1))

  // change format
  private def getDecayedCountsAsync(policy: Counter,
                                    items: Seq[String],
                                    timeRange: (TimedQualifier, TimedQualifier),
                                    dimension: Map[String, String],
                                    qsSum: Option[String]): Future[Seq[(ExactKeyTrait, Double)]] = {
    exactCounter(policy.version).getDecayedCountsAsync(policy, items, Seq(timeRange), dimension.mapValues(s => Set(s)), qsSum).map { seq =>
      for {
        DecayedCounts(exactKey, qcMap) <- seq
        value <- qcMap.values
      } yield {
        exactKey -> value
      }
    }
  }

  def getSumRankCounterResultAsync(policy: Counter,
                                   dimKeys: Seq[(Map[String, String], Seq[RankingKey])],
                                   kValue: Int,
                                   qsSum: Option[String]): Future[RankCounterResult] = {
    val futures = {
      for {
        (dimension, keys) <- dimKeys
      } yield {
        val tqs = keys.map(rk => rk.eq.tq)
        val (tqFrom, tqTo) = (tqs.last, tqs.head)
        val items = rankingCounter(policy.version).getAllItems(keys, kValue)
//        Logger.warn(s"item count: ${items.length}")
        val future = {
          if (policy.isRateCounter) {
            val actionPolicy = policy.rateActionId.flatMap(counterModel.findById(_)).get
            val basePolicy = policy.rateBaseId.flatMap(counterModel.findById(_)).get

            val futureAction = getDecayedCountsAsync(actionPolicy, items, (tqFrom, tqTo), dimension, qsSum).map { seq =>
              seq.map { case (k, score) =>
                ExactKey(policy, k.itemKey, checkItemType = false) -> RateRankingValue(score, -1)
              }.toMap
            }
            val futureBase = getDecayedCountsAsync(basePolicy, items, (tqFrom, tqTo), dimension, qsSum).map { seq =>
              seq.map { case (k, score) =>
                ExactKey(policy, k.itemKey, checkItemType = false) -> RateRankingValue(-1, score)
              }.toMap
            }
            futureAction.zip(futureBase).map { case (actionScores, baseScores) =>
              reduceRateRankingValue(actionScores, baseScores).map { case (k, rrv) =>
//                Logger.warn(s"$k -> $rrv")
                k -> rrv.rankingValue.score
              }.toSeq
            }
          }
          else if (policy.isTrendCounter) {
            val actionPolicy = policy.rateActionId.flatMap(counterModel.findById(_)).get
            val basePolicy = policy.rateBaseId.flatMap(counterModel.findById(_)).get

            val futureAction = getDecayedCountsAsync(actionPolicy, items, (tqFrom, tqTo), dimension, qsSum).map { seq =>
              seq.map { case (k, score) =>
                ExactKey(policy, k.itemKey, checkItemType = false) -> RateRankingValue(score, -1)
              }.toMap
            }
            val futureBase = getDecayedCountsAsync(basePolicy, items, (tqFrom.add(-1), tqTo.add(-1)), dimension, qsSum).map { seq =>
              seq.map { case (k, score) =>
                ExactKey(policy, k.itemKey, checkItemType = false) -> RateRankingValue(-1, score)
              }.toMap
            }
            futureAction.zip(futureBase).map { case (actionScores, baseScores) =>
              reduceRateRankingValue(actionScores, baseScores).map { case (k, rrv) =>
//                Logger.warn(s"$k -> $rrv")
                k -> rrv.rankingValue.score
              }.toSeq
            }
          }
          else {
            getDecayedCountsAsync(policy, items, (tqFrom, tqTo), dimension, qsSum)
          }
        }
        future.map { keyWithScore =>
          val ranking = keyWithScore.sortBy(-_._2).take(kValue)
          val rankCounterItems = {
            for {
              idx <- ranking.indices
              (exactKey, score) = ranking(idx)
            } yield {
              val realId = policy.itemType match {
                case ItemType.BLOB => exactCounter(policy.version).getBlobValue(policy, exactKey.itemKey)
                  .getOrElse(throw new Exception(s"not found blob id. ${policy.service}.${policy.action} ${exactKey.itemKey}"))
                case _ => exactKey.itemKey
              }
              RankCounterItem(idx + 1, realId, score)
            }
          }

          val eq = ExactQualifier(tqFrom, dimension)
          RankCounterDimensionItem(eq.tq.q.toString, eq.tq.ts, eq.dimension, -1, rankCounterItems)
        }
      }
    }

    Future.sequence(futures).map { dimensionResultList =>
      RankCounterResult(RankCounterResultMeta(policy.service, policy.action), dimensionResultList)
    }
  }

  def getRankCounterResult(policy: Counter, dimKeys: Seq[(Map[String, String], Seq[RankingKey])], kValue: Int): RankCounterResult = {
    val dimensionResultList = {
      for {
        (dimension, keys) <- dimKeys
        key <- keys
      } yield {
        val rankingValue = rankingCounter(policy.version).getTopK(key, kValue)
        val ranks = {
          for {
            rValue <- rankingValue.toSeq
            idx <- rValue.values.indices
            rank = idx + 1
          } yield {
            val (id, score) = rValue.values(idx)
            val realId = policy.itemType match {
              case ItemType.BLOB =>
                exactCounter(policy.version)
                  .getBlobValue(policy, id)
                  .getOrElse(throw new Exception(s"not found blob id. ${policy.service}.${policy.action} $id"))
              case _ => id
            }
            RankCounterItem(rank, realId, score)
          }
        }
        val eq = key.eq
        val tq = eq.tq
        RankCounterDimensionItem(tq.q.toString, tq.ts, eq.dimension, rankingValue.map(v => v.totalScore).getOrElse(0d), ranks)
      }
    }

    RankCounterResult(RankCounterResultMeta(policy.service, policy.action), dimensionResultList)
  }

  type Record = ProducerRecord[String, String]

  def incrementCount(service: String, action: String, item: String) = Action.async(s2parse.json) { request =>
    Future {
      /**
       * {
       * timestamp: Long
       * property: {}
       * value: Int
       * }
       */
      lazy val metaMap = Map("service" -> service, "action" -> action, "item" -> item)
      counterModel.findByServiceAction(service, action).map { policy =>
        val body = request.body
        try {
          val ts = (body \ "timestamp").asOpt[Long].getOrElse(System.currentTimeMillis()).toString
          val dimension = (body \ "dimension").asOpt[JsValue].getOrElse(Json.obj())
          val property = (body \ "property").asOpt[JsValue].getOrElse(Json.obj())

          val msg = List(ts, service, action, item, dimension, property).mkString("\t")

          // produce to kafka
          // hash partitioner by key
          ExceptionHandler.enqueue(KafkaMessage(new Record(CounterConfig.KAFKA_TOPIC_COUNTER, s"$ts.$item", msg)))

          Ok(Json.toJson(
            Map(
              "meta" -> metaMap
            )
          ))
        }
        catch {
          case e: JsResultException =>
            BadRequest(Json.toJson(
              Map("msg" -> s"need timestamp.")
            ))
        }
      }.getOrElse {
        NotFound(Json.toJson(
          Map("msg" -> s"$service.$action not found")
        ))
      }
    }
  }
}
