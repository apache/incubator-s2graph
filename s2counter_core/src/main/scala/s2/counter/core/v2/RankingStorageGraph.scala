package s2.counter.core.v2

import com.kakao.s2graph.core.GraphUtil
import com.kakao.s2graph.core.mysqls.Label
import com.typesafe.config.Config
import org.apache.commons.httpclient.HttpStatus
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import s2.config.S2CounterConfig
import s2.counter.core.RankingCounter.RankingValueMap
import s2.counter.core.{RankingKey, RankingResult, RankingStorage}
import s2.models.{Counter, CounterModel}
import s2.util.{CollectionCache, CollectionCacheConfig}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.util.hashing.MurmurHash3

/**
 * Created by shon on 7/28/15.
 */
object RankingStorageGraph {
  // using play-ws without play app
  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val wsClient = new play.api.libs.ws.ning.NingWSClient(builder.build)
}

class RankingStorageGraph(config: Config) extends RankingStorage {
  import RankingStorageGraph._

  private[counter] val log = LoggerFactory.getLogger(this.getClass)
  private val s2config = new S2CounterConfig(config)

  private val BUCKET_SHARD_COUNT = 1
  private val SERVICE_NAME = "s2counter"
  private val BUCKET_COLUMN_NAME = "bucket"
  private val counterModel = new CounterModel(config)
  private val labelPostfix = "_topK"

  val s2graphUrl = s2config.GRAPH_URL
  val s2graphReadOnlyUrl = s2config.GRAPH_READONLY_URL

  val prepareCache = new CollectionCache[Option[Boolean]](CollectionCacheConfig(10000, 600))
  val graphOp = new GraphOperation(config)
  import scala.concurrent.ExecutionContext.Implicits.global

  private def makeBucketKey(rankingKey: RankingKey): String = {
    val eq = rankingKey.eq
    val tq = eq.tq
    s"${tq.q}.${tq.ts}.${eq.dimension}"
  }

  // "", "age.32", "age.gender.32.M"
  private def makeBucketShardKey(shardIdx: Int, rankingKey: RankingKey): String = {
    s"$shardIdx.${makeBucketKey(rankingKey)}"
  }

  /**
   * indexProps: ["time_unit", "time_value", "score"]
   */
  override def getTopK(key: RankingKey, k: Int): Option[RankingResult] = {
    getTopK(Seq(key), k).headOption.map(_._2)
  }

  override def getTopK(keys: Seq[RankingKey], k: Int): Seq[(RankingKey, RankingResult)] = {
    val futures = for {
      key <- keys
    } yield {
      getEdges(key).map { edges =>
        key -> RankingResult(0d, toWithScoreLs(edges).take(k))
      }
    }

    Await.result(Future.sequence(futures), 10 seconds)
  }

  override def update(key: RankingKey, value: RankingValueMap, k: Int): Unit = {
    update(Seq((key, value)), k)
  }

  override def update(values: Seq[(RankingKey, RankingValueMap)], k: Int): Unit = {
    val futures = {
      for {
        (key, value) <- values
      } yield {
        // prepare dimension bucket edge
        if (checkAndPrepareDimensionBucket(key)) {
          val future = getEdges(key, "raw").flatMap { edges =>
            val prevRankingSeq = toWithScoreLs(edges)
            val prevRankingMap: Map[String, Double] = prevRankingSeq.groupBy(_._1).map(_._2.sortBy(-_._2).head)
            val currentRankingMap: Map[String, Double] = value.mapValues(_.score)
            val mergedRankingSeq = (prevRankingMap ++ currentRankingMap).toSeq.sortBy(-_._2).take(k)
            val mergedRankingMap = mergedRankingSeq.toMap

            val bucketRankingSeq = mergedRankingSeq.groupBy { case (itemId, score) =>
              // 0-index
              GraphUtil.transformHash(MurmurHash3.stringHash(itemId)) % BUCKET_SHARD_COUNT
            }.map { case (shardIdx, groupedRanking) =>
              shardIdx -> groupedRanking.filter { case (itemId, _) => currentRankingMap.contains(itemId) }
            }.toSeq

            insertBulk(key, bucketRankingSeq).flatMap { _ =>
              val duplicatedItems = prevRankingMap.filterKeys(s => currentRankingMap.contains(s))
              val cutoffItems = prevRankingMap.filterKeys(s => !mergedRankingMap.contains(s))
              val deleteItems = duplicatedItems ++ cutoffItems

              val keyWithEdgesLs = prevRankingSeq.map(_._1).zip(edges)
              val deleteEdges = keyWithEdgesLs.filter{ case (s, _) => deleteItems.contains(s) }.map(_._2)

              deleteAll(deleteEdges)
            }
          }

          future
        }
        else {
          // do nothing
          Future.successful(false)
        }
      }
    }

    Await.result(Future.sequence(futures), 10 seconds)
  }

  private def toWithScoreLs(edges: List[JsValue]): List[(String, Double)] = {
    for {
      edgeJson <- edges
      to = (edgeJson \ "to").as[JsValue]
      score = (edgeJson \ "score").as[JsValue].toString().toDouble
    } yield {
      val toValue = to match {
        case s: JsString => s.as[String]
        case _ => to.toString()
      }
      toValue -> score
    }
  }

  private def insertBulk(key: RankingKey, newRankingSeq: Seq[(Int, Seq[(String, Double)])]): Future[Boolean] = {
    val labelName = counterModel.findById(key.policyId).get.action + labelPostfix
    val timestamp: Long = System.currentTimeMillis
    val payload = Json.toJson {
      for {
        (shardIdx, rankingSeq) <- newRankingSeq
        (itemId, score) <- rankingSeq
      } yield {
        val srcId = makeBucketShardKey(shardIdx, key)
        Json.obj(
          "timestamp" -> timestamp,
          "from" -> srcId,
          "to" -> itemId,
          "label" -> labelName,
          "props" -> Json.obj(
            "time_unit" -> key.eq.tq.q.toString,
            "time_value" -> key.eq.tq.ts,
            "date_time" -> key.eq.tq.dateTime,
            "dimension" -> key.eq.dimension,
            "score" -> score
          )
        )
      }
    }

    wsClient.url(s"$s2graphUrl/graphs/edges/insertBulk").post(payload).map { resp =>
      resp.status match {
        case HttpStatus.SC_OK =>
          true
        case _ =>
          throw new RuntimeException(s"failed insertBulk. errCode: ${resp.status}, body: ${resp.body}, query: $payload")
      }
    }
  }

  private def deleteAll(edges: List[JsValue]): Future[Boolean] = {
    // /graphs/edges/delete
    val futures = {
      for {
        groupedEdges <- edges.grouped(50)
      } yield {
        val payload = Json.toJson(groupedEdges)
        wsClient.url(s"$s2graphUrl/graphs/edges/delete").post(payload).map { resp =>
          resp.status match {
            case HttpStatus.SC_OK =>
              true
            case _ =>
              log.error(s"failed delete. errCode: ${resp.status}, body: ${resp.body}, query: $payload")
              false
          }
        }
      }
    }.toSeq

    Future.sequence(futures).map { seq =>
      seq.forall(x => x)
    }
  }

  /** select and delete */
  override def delete(key: RankingKey): Unit = {
    val future = getEdges(key).flatMap { edges =>
      deleteAll(edges)
    }
    Await.result(future, 10 second)
  }

  private def getEdges(key: RankingKey, duplicate: String="first"): Future[List[JsValue]] = {
    val labelName = counterModel.findById(key.policyId).get.action + labelPostfix

//    val ids = (0 until BUCKET_SHARD_COUNT).map { idx =>
//      s"${makeBucketShardKey(idx, key)}"
//    }
//
//    val payload = Json.obj(
//      "srcVertices" -> Json.arr(
//        Json.obj(
//          "serviceName" -> SERVICE_NAME,
//          "columnName" -> BUCKET_COLUMN_NAME,
//          "ids" -> ids
//        )
//      ),
//      "steps" -> Json.arr(
//        Json.obj(
//          "step" -> Json.arr(
//            Json.obj(
//              "label" -> labelName,
//              "duplicate" -> duplicate,
//              "direction" -> "out",
//              "offset" -> 0,
//              "limit" -> -1,
//              "interval" -> Json.obj(
//                "from" -> Json.obj(
//                  "time_unit" -> key.eq.tq.q.toString,
//                  "time_value" -> key.eq.tq.ts
//                ),
//                "to" -> Json.obj(
//                  "time_unit" -> key.eq.tq.q.toString,
//                  "time_value" -> key.eq.tq.ts
//                ),
//                "scoring" -> Json.obj(
//                  "score" -> 1
//                )
//              )
//            )
//          )
//        )
//      )
//    )

    val ids = {
      (0 until BUCKET_SHARD_COUNT).map { shardIdx =>
        s""""${makeBucketShardKey(shardIdx, key)}""""
      }
    }.mkString(",")

    val strJs =
      s"""
         |{
         |    "srcVertices": [
         |        {
         |            "serviceName": "$SERVICE_NAME",
         |            "columnName": "$BUCKET_COLUMN_NAME",
         |            "ids": [$ids]
         |        }
         |    ],
         |    "steps": [
         |        {
         |            "step": [
         |                {
         |                    "label": "$labelName",
         |                    "duplicate": "$duplicate",
         |                    "direction": "out",
         |                    "offset": 0,
         |                    "limit": -1,
         |                    "interval": {
         |                      "from": {"time_unit": "${key.eq.tq.q.toString}", "time_value": ${key.eq.tq.ts}},
         |                      "to": {"time_unit": "${key.eq.tq.q.toString}", "time_value": ${key.eq.tq.ts}}
         |                    },
         |                    "scoring": {"score": 1}
         |                }
         |            ]
         |        }
         |    ]
         |}
       """.stripMargin
    log.debug(strJs)

    Try {
      Json.parse(strJs)
    } match {
      case Success(payload) =>
        wsClient.url(s"$s2graphReadOnlyUrl/graphs/getEdges").post(payload).map { resp =>
          resp.status match {
            case HttpStatus.SC_OK =>
              (resp.json \ "results").asOpt[List[JsValue]].getOrElse(Nil)
            case _ =>
              throw new RuntimeException(s"failed getEdges. errCode: ${resp.status}, body: ${resp.body}, query: $payload")
          }
        }
      case Failure(ex) =>
        log.error(s"$ex")
        Future.successful(Nil)
    }
  }

  private def existsLabel(policy: Counter, useCache: Boolean = true): Boolean = {
    val action = policy.action
    val counterLabelName = action + labelPostfix

    Label.findByName(counterLabelName, useCache).nonEmpty
  }

  private def checkAndPrepareDimensionBucket(rankingKey: RankingKey): Boolean = {
    val dimension = rankingKey.eq.dimension
    val bucketKey = makeBucketKey(rankingKey)
    val labelName = "s2counter_topK_bucket"

    val prepared = prepareCache.withCache(s"$dimension:$bucketKey") {
      val checkReqJs = Json.arr(
        Json.obj(
          "label" -> labelName,
          "direction" -> "out",
          "from" -> dimension,
          "to" -> makeBucketShardKey(BUCKET_SHARD_COUNT - 1, rankingKey)
        )
      )

      val future = wsClient.url(s"$s2graphReadOnlyUrl/graphs/checkEdges").post(checkReqJs).map { resp =>
        resp.status match {
          case HttpStatus.SC_OK =>
            val checkRespJs = resp.json
            if (checkRespJs.as[Seq[JsValue]].nonEmpty) {
              true
            } else {
              false
            }
          case _ =>
            // throw exception
            throw new Exception(s"failed checkEdges. ${resp.body} ${resp.status}")
        }
      }.flatMap {
        case true => Future.successful(Some(true))
        case false =>
          val insertReqJsLs = {
            for {
              i <- 0 until BUCKET_SHARD_COUNT
            } yield {
              Json.obj(
                "timestamp" -> rankingKey.eq.tq.ts,
                "from" -> dimension,
                "to" -> makeBucketShardKey(i, rankingKey),
                "label" -> labelName,
                "props" -> Json.obj(
                  "time_unit" -> rankingKey.eq.tq.q.toString,
                  "date_time" -> rankingKey.eq.tq.dateTime
                )
              )
            }
          }
          wsClient.url(s"$s2graphUrl/graphs/edges/insert").post(Json.toJson(insertReqJsLs)).map { resp =>
            resp.status match {
              case HttpStatus.SC_OK =>
                Some(true)
              case _ =>
                // throw exception
                throw new Exception(s"failed insertEdges. ${resp.body} ${resp.status}")
            }
          }
      }.recover {
        case e: Exception =>
          log.error(s"$e")
          None
      }

      Await.result(future, 10 second)
    }
    prepared.getOrElse(false)
  }

  override def prepare(policy: Counter): Unit = {
    val service = policy.service
    val action = policy.action

    val graphLabel = {
      policy.rateActionId match {
        case Some(rateId) =>
          counterModel.findById(rateId, useCache = false).flatMap { ratePolicy =>
            Label.findByName(ratePolicy.action)
          }
        case None =>
          Label.findByName(action)
      }
    }
    if (graphLabel.isEmpty) {
      throw new Exception(s"label not found. $service.$action")
    }

    if (!existsLabel(policy, useCache = false)) {
      // find input label to specify target column
      val label = graphLabel.get

      val counterLabelName = action + labelPostfix
      val defaultJson =
        s"""
           |{
           |  "label": "$counterLabelName",
           |  "srcServiceName": "$SERVICE_NAME",
           |  "srcColumnName": "$BUCKET_COLUMN_NAME",
           |  "srcColumnType": "string",
           |  "tgtServiceName": "$service",
           |  "tgtColumnName": "${label.tgtColumnName}",
           |  "tgtColumnType": "${label.tgtColumnType}",
           |  "indices": [
           |    {"name": "time", "propNames": ["time_unit", "time_value", "score"]}
           |  ],
           |  "props": [
           |    {"name": "time_unit", "dataType": "string", "defaultValue": ""},
           |    {"name": "time_value", "dataType": "long", "defaultValue": 0},
           |    {"name": "date_time", "dataType": "long", "defaultValue": 0},
           |    {"name": "score", "dataType": "float", "defaultValue": 0.0}
           |  ],
           |  "hTableName": "${policy.hbaseTable.get}",
           |  "schemaVersion": "${label.schemaVersion}"
           |}
         """.stripMargin
      val json = policy.dailyTtl.map(ttl => ttl * 24 * 60 * 60) match {
        case Some(ttl) =>
          Json.parse(defaultJson).as[JsObject] + ("hTableTTL" -> Json.toJson(ttl))
        case None =>
          Json.parse(defaultJson)
      }

      graphOp.createLabel(json)
    }
  }

  override def destroy(policy: Counter): Unit = {
    val action = policy.action

    if (existsLabel(policy, useCache = false)) {
      val counterLabelName = action + labelPostfix

      graphOp.deleteLabel(counterLabelName)
    }
  }

  override def ready(policy: Counter): Boolean = {
    existsLabel(policy)
  }
}
