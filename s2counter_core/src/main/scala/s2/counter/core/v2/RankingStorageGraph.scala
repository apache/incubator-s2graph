package s2.counter.core.v2

import com.kakao.s2graph.core.GraphUtil
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core.types.HBaseType
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import s2.config.S2CounterConfig
import s2.counter.core.RankingCounter.RankingValueMap
import s2.counter.core.{RankingKey, RankingResult, RankingStorage}
import s2.models.{Counter, CounterModel}

import scala.util.hashing.MurmurHash3
import scalaj.http.{Http, HttpResponse}

/**
 * Created by shon on 7/28/15.
 */
case class RankingStorageGraph(config: Config) extends RankingStorage {
  private[counter] val log = LoggerFactory.getLogger(this.getClass)
  private val s2config = new S2CounterConfig(config)

  private val K_MAX = 500
//  private val RANK_INTERVAL = 10
  private val BUCKET_COUNT = 53
  private val SERVICE_NAME = "s2counter"
  private val COLUMN_NAME = "bucket"
  private val counterModel = new CounterModel(config)
  private val labelPostfix = "_topK"

  val s2graphUrl = s2config.GRAPH_URL

  // "", "age.32", "age.gender.32.M"
  private def makeBucketSimple(rankingKey: RankingKey, bucketIdx: Int): String = {
//    val labelName = counterModel.findById(rankingKey.policyId).get.action
//    val q = rankingKey.eq.tq.q
//    val ts = rankingKey.eq.tq.ts
    val dimension = rankingKey.eq.dimension
    s"$bucketIdx.$dimension"
  }

  /**
   * indexProps: ["time_unit", "time_value", "score"]
   */
  override def getTopK(key: RankingKey, k: Int): Option[RankingResult] = {
    val edges = getEdges(key)
    val values = toWithScoreLs(edges).take(k)
    log.debug(edges.toString())
    Some(RankingResult(0d, values))
  }

  override def getTopK(keys: Seq[RankingKey], k: Int): Seq[(RankingKey, RankingResult)] = {
    for {
      key <- keys
      result <- getTopK(key, k)
    } yield {
      (key, result)
    }
  }

  override def update(key: RankingKey, value: RankingValueMap, k: Int): Unit = {
    update(Seq((key, value)), k)
  }

  override def update(values: Seq[(RankingKey, RankingValueMap)], k: Int): Unit = {
    val respLs = {
      for {
        (key, value) <- values
      } yield {
        val edges = getEdges(key, "raw")

        val prevRankingSeq = toWithScoreLs(edges)
        val prevRankingMap: Map[String, Double] = prevRankingSeq.groupBy(_._1).map(_._2.sortBy(-_._2).head)
        val currentRankingMap: Map[String, Double] = value.mapValues(_.score)
        val mergedRankingSeq = (prevRankingMap ++ currentRankingMap).toSeq.sortBy(-_._2).take(k)
        val mergedRankingMap = mergedRankingSeq.toMap

        val bucketRankingSeq = mergedRankingSeq.groupBy { case (itemId, score) =>
          // 0-index
          GraphUtil.transformHash(MurmurHash3.stringHash(itemId)) % BUCKET_COUNT
        }.map { case (bucketIdx, groupedRanking) =>
          bucketIdx -> groupedRanking.filter { case (itemId, _) => currentRankingMap.contains(itemId) }
        }.toSeq

        insertBulk(key, bucketRankingSeq)

        val duplicatedItems = prevRankingMap.filterKeys(s => currentRankingMap.contains(s))
        val cutoffItems = prevRankingMap.filterKeys(s => !mergedRankingMap.contains(s))
        val deleteItems = duplicatedItems ++ cutoffItems

        val keyWithEdgesMap = prevRankingSeq.map(_._1).zip(edges)
        val deleteEdges = keyWithEdgesMap.filter{ case (s, _) => deleteItems.contains(s) }.map(_._2)
        deleteAll(deleteEdges)
      }
    }
    if (!respLs.forall(resp => resp.isSuccess)) {
      val keys = values.map(_._1)
      keys.zip(respLs).filter(_._2.isError).foreach { case (key, resp) =>
        log.error(s"$key: $resp")
      }
    }
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

  private def insertBulk(key: RankingKey, newRankingSeq: Seq[(Int, Seq[(String, Double)])]): HttpResponse[String] = {
    val labelName = counterModel.findById(key.policyId).get.action + labelPostfix
    val timestamp: Long = System.currentTimeMillis
    val events = {
      for {
        (bucketIdx, rankingSeq) <- newRankingSeq
        (itemId, score) <- rankingSeq
      } yield {
        val srcId = makeBucketSimple(key, bucketIdx)
        Json.obj(
          "timestamp" -> timestamp,
          "from" -> srcId,
          "to" -> itemId,
          "label" -> labelName,
          "props" -> Json.obj(
            "time_unit" -> key.eq.tq.q.toString,
            "time_value" -> key.eq.tq.ts,
            "score" -> score
          )
        )
      }
    }
    val jsonStr = Json.toJson(events).toString()
//    log.warn(jsonStr)
    val resp = Http(s"$s2graphUrl/graphs/edges/insertBulk")
      .postData(jsonStr)
      .header("content-type", "application/json").asString
    if (resp.isError) {
      log.error(s"errCode: ${resp.code}, body: ${resp.body}, query: $jsonStr")
    }
    resp
  }

  private def deleteAll(edges: List[JsValue]) = {
    // /graphs/edges/delete
    val payload = Json.toJson(edges).toString()
    Http(s"$s2graphUrl/graphs/edges/delete")
      .postData(payload)
      .header("content-type", "application/json").execute()
  }

  /** select and delete */
  override def delete(key: RankingKey): Unit = {
    val offset = 0
    val limit = K_MAX
    val edges = getEdges(key)
    deleteAll(edges)
  }

  private def getEdges(key: RankingKey, duplicate: String="first"): List[JsValue] = {
    val labelName = counterModel.findById(key.policyId).get.action + labelPostfix

    val ids = {
//      val intervals = limit / RANK_INTERVAL + { if (limit % RANK_INTERVAL != 0) 1 else 0 }
      (0 until BUCKET_COUNT).map { bucketIdx =>
        s""""${makeBucketSimple(key, bucketIdx)}""""
      }
    }.mkString(",")

    val json =
      s"""
         |{
         |    "srcVertices": [
         |        {
         |            "serviceName": "$SERVICE_NAME",
         |            "columnName": "$COLUMN_NAME",
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

    log.debug(json)

    val response = Http(s"$s2graphUrl/graphs/getEdges")
      .postData(json)
      .header("content-type", "application/json").asString

    (Json.parse(response.body) \ "results").asOpt[List[JsValue]].getOrElse(Nil)
  }

  private def existsLabel(policy: Counter): Boolean = {
    val action = policy.action
    val counterLabelName = action + labelPostfix

    Label.findByName(counterLabelName).nonEmpty
  }

  override def prepare(policy: Counter): Unit = {
    val service = policy.service
    val action = policy.action

    if (!existsLabel(policy)) {
      // find input label to specify target column
      val inputLabelName = policy.rateActionId.flatMap { id =>
        counterModel.findById(id, useCache = false).map(_.action)
      }.getOrElse(action)
      val defaultLabel = Label(None, inputLabelName, -1, "", "", -1, "s2counter_id", policy.itemType.toString.toLowerCase,
        isDirected = true, service, -1, "weak", "", None, HBaseType.DEFAULT_VERSION, isAsync = false, "lz4")
      val label = Label.findByName(inputLabelName, useCache = false)
        .getOrElse(defaultLabel)

      val counterLabelName = action + labelPostfix
      val defaultJson =
        s"""
           |{
           |	"label": "$counterLabelName",
           |	"srcServiceName": "$SERVICE_NAME",
           |	"srcColumnName": "$COLUMN_NAME",
           |	"srcColumnType": "string",
           |	"tgtServiceName": "$service",
           |	"tgtColumnName": "${label.tgtColumnName}",
           |	"tgtColumnType": "${label.tgtColumnType}",
           |	"indices": [
           |    {"name": "time", "propNames": ["time_unit", "time_value", "score"]}
           |	],
           |  "props": [
           |		{"name": "time_unit", "dataType": "string", "defaultValue": ""},
           |		{"name": "time_value", "dataType": "long", "defaultValue": 0},
           |		{"name": "score", "dataType": "float", "defaultValue": 0.0}
           |  ],
           |  "hTableName": "${policy.hbaseTable.get}"
           |}
     """.stripMargin
      val json = policy.dailyTtl.map(ttl => ttl * 24 * 60 * 60) match {
        case Some(ttl) =>
          Json.parse(defaultJson).as[JsObject] + ("hTableTTL" -> Json.toJson(ttl)) toString()
        case None =>
          defaultJson
      }

      val response = Http(s"$s2graphUrl/graphs/createLabel")
        .postData(json)
        .header("content-type", "application/json").asString

      if (response.isError) {
        throw new RuntimeException(s"$json ${response.code} ${response.body}")
      }
    }
  }

  override def destroy(policy: Counter): Unit = {
    val action = policy.action

    if (existsLabel(policy)) {
      val counterLabelName = action + labelPostfix

      val response = Http(s"$s2graphUrl/graphs/deleteLabel/$counterLabelName").method("PUT").asString

      if (response.isError) {
        throw new RuntimeException(s"${response.code} ${response.body}")
      }
    }
  }

  override def ready(policy: Counter): Boolean = {
    existsLabel(policy)
  }
}
