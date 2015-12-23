package s2.counter.core.v2

import com.kakao.s2graph.core.mysqls.Label
import com.typesafe.config.Config
import org.apache.http.HttpStatus
import org.slf4j.LoggerFactory
import play.api.libs.json._
import s2.config.S2CounterConfig
import s2.counter.core.ExactCounter.ExactValueMap
import s2.counter.core._
import s2.models.Counter
import s2.util.CartesianProduct

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 8. 19..
 */
object ExactStorageGraph {
  case class RespGraph(success: Boolean, result: Long)
  implicit val respGraphFormat = Json.format[RespGraph]

  // using play-ws without play app
  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val wsClient = new play.api.libs.ws.ning.NingWSClient(builder.build)
}

case class ExactStorageGraph(config: Config) extends ExactStorage {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val s2config = new S2CounterConfig(config)

  private val SERVICE_NAME = "s2counter"
  private val COLUMN_NAME = "bucket"
  private val labelPostfix = "_counts"

  val s2graphUrl = s2config.GRAPH_URL
  val s2graphReadOnlyUrl = s2config.GRAPH_READONLY_URL
  val graphOp = new GraphOperation(config)

  import ExactStorageGraph._

  override def update(policy: Counter, counts: Seq[(ExactKeyTrait, ExactValueMap)]): Map[ExactKeyTrait, ExactValueMap] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val futureLs = {
      for {
        requests <- toIncrementCountRequests(policy, counts).grouped(10)
        (keyWithEq, reqJsLs) = requests.unzip(x => ((x._1, x._2), x._3))
      } yield {
        wsClient.url(s"$s2graphUrl/graphs/edges/incrementCount").post(Json.toJson(reqJsLs)).map { resp =>
          resp.status match {
            case HttpStatus.SC_OK =>
              val respSeq = resp.json.as[Seq[RespGraph]]

              for {
                ((key, eq), RespGraph(success, result)) <- keyWithEq.zip(respSeq)
              } yield {
                (key, (eq, result))
              }
            case HttpStatus.SC_BAD_REQUEST =>
              // logging
              log.error(s"BAD_REQUEST: $policy $counts")
              Nil
            case rc: Int =>
//              throw new RuntimeException(s"update failed($rc): $policy $counts")
              log.error(s"update failed($rc): $policy $counts")
              Nil
          }
        }
      }
    }

    val future = Future.sequence(futureLs).map { seqOfSeq =>
      seqOfSeq.flatten.toSeq.groupBy(_._1).mapValues { seq => seq.map(_._2).toMap }
    }
    Await.result(future, 10 second)
  }

  def delete(policy: Counter, keys: Seq[ExactKeyTrait]): Unit = {

  }

  private def toIncrementCountRequests(policy: Counter,
                                       counts: Seq[(ExactKeyTrait, ExactValueMap)])
  : Seq[(ExactKeyTrait, ExactQualifier, JsValue)] = {
    val labelName = policy.action + labelPostfix
    val timestamp = System.currentTimeMillis()
    for {
      (exactKey, values) <- counts
      (eq, value) <- values
    } yield {
      val from = exactKey.itemKey
      val to = eq.dimension
      val json = Json.obj(
        "timestamp" -> timestamp,
        "operation" -> "incrementCount",
        "from" -> from,
        "to" -> to,
        "label" -> labelName,
        "props" -> Json.obj(
          "_count" -> value,
          "time_unit" -> eq.tq.q.toString,
          "time_value" -> eq.tq.ts
        )
      )
      (exactKey, eq, json)
    }
  }

  override def get(policy: Counter,
                   items: Seq[String],
                   timeRange: Seq[(TimedQualifier, TimedQualifier)],
                   dimQuery: Map[String, Set[String]])
                  (implicit ex: ExecutionContext): Future[Seq[FetchedCountsGrouped]] = {
    val labelName = policy.action + labelPostfix
    val label = Label.findByName(labelName).get
//    val label = labelModel.findByName(labelName).get

    val ids = Json.toJson(items)

    val dimensions = {
      for {
        values <- CartesianProduct(dimQuery.values.map(ss => ss.toList).toList)
      } yield {
        dimQuery.keys.zip(values).toMap
      }
    }

    val stepJsLs = {
      for {
        (tqFrom, tqTo) <- timeRange
        dimension <- dimensions
      } yield {
        val eqFrom = ExactQualifier(tqFrom, dimension)
        val eqTo = ExactQualifier(tqTo, dimension)
        val intervalJs =
          s"""
            |{
            |  "from": {
            |    "_to": "${eqFrom.dimension}",
            |    "time_unit": "${eqFrom.tq.q}",
            |    "time_value": ${eqFrom.tq.ts}
            |  },
            |  "to": {
            |    "_to": "${eqTo.dimension}",
            |    "time_unit": "${eqTo.tq.q}",
            |    "time_value": ${eqTo.tq.ts + 1}
            |  }
            |}
          """.stripMargin
        val stepJs =
          s"""
            |{
            |  "direction": "out",
            |  "limit": -1,
            |  "duplicate": "raw",
            |  "label": "$labelName",
            |  "interval": $intervalJs
            |}
           """.stripMargin
        stepJs
      }
    }

    val reqJsStr =
      s"""
        |{
        |  "srcVertices": [
        |    {"serviceName": "${policy.service}", "columnName": "${label.srcColumnName}", "ids": $ids}
        |  ],
        |  "steps": [
        |    {
        |      "step": [
        |        ${stepJsLs.mkString(",")}
        |      ]
        |    }
        |  ]
        |}
      """.stripMargin

    val reqJs = Json.parse(reqJsStr)
//    log.warn(s"query: ${reqJs.toString()}")

    wsClient.url(s"$s2graphReadOnlyUrl/graphs/getEdges").post(reqJs).map { resp =>
      resp.status match {
        case HttpStatus.SC_OK =>
          val respJs = resp.json
//          println(respJs)
          val keyWithValues = (respJs \ "results").as[Seq[JsValue]].map { result =>
//            println(s"result: $result")
            resultToExactKeyValues(policy, result)
          }.groupBy(_._1).mapValues(seq => seq.map(_._2).toMap.groupBy { case (eq, v) => (eq.tq.q, eq.dimKeyValues) })
          for {
            (k, v) <- keyWithValues.toSeq
          } yield {
            FetchedCountsGrouped(k, v)
          }
        case n: Int =>
          log.warn(s"getEdges status($n): $reqJsStr")
//          println(s"getEdges status($n): $reqJsStr")
          Nil
      }
    }
  }

  private def resultToExactKeyValues(policy: Counter, result: JsValue): (ExactKeyTrait, (ExactQualifier, Long)) = {
    val from = result \ "from" match {
      case s: JsString => s.as[String]
      case n: JsNumber => n.as[Long].toString
      case x: JsValue => throw new RuntimeException(s"$x's type must be string or number")
    }
    val dimension = (result \ "to").as[String]
    val props = result \ "props"
    val count = (props \ "_count").as[Long]
    val timeUnit = (props \ "time_unit").as[String]
    val timeValue = (props \ "time_value").as[Long]
    (ExactKey(policy, from, checkItemType = true), (ExactQualifier(TimedQualifier(timeUnit, timeValue), dimension), count))
  }

  private def getInner(policy: Counter, key: ExactKeyTrait, eqs: Seq[ExactQualifier])
                      (implicit ex: ExecutionContext): Future[Seq[FetchedCounts]] = {
    val labelName = policy.action + labelPostfix

    Label.findByName(labelName) match {
      case Some(label) =>

        val src = Json.obj("serviceName" -> policy.service, "columnName" -> label.srcColumnName, "id" -> key.itemKey)
        val step = {
          val stepLs = {
            for {
              eq <- eqs
            } yield {
              val from = Json.obj("_to" -> eq.dimension, "time_unit" -> eq.tq.q.toString, "time_value" -> eq.tq.ts)
              val to = Json.obj("_to" -> eq.dimension, "time_unit" -> eq.tq.q.toString, "time_value" -> eq.tq.ts)
              val interval = Json.obj("from" -> from, "to" -> to)
              Json.obj("limit" -> 1, "label" -> labelName, "interval" -> interval)
            }
          }
          Json.obj("step" -> stepLs)
        }
        val query = Json.obj("srcVertices" -> Json.arr(src), "steps" -> Json.arr(step))
        //    println(s"query: ${query.toString()}")

        wsClient.url(s"$s2graphReadOnlyUrl/graphs/getEdges").post(query).map { resp =>
          resp.status match {
            case HttpStatus.SC_OK =>
              val respJs = resp.json
              val keyWithValues = (respJs \ "results").as[Seq[JsValue]].map { result =>
                resultToExactKeyValues(policy, result)
              }.groupBy(_._1).mapValues(seq => seq.map(_._2).toMap)
              for {
                (key, eqWithValues) <- keyWithValues.toSeq
              } yield {
                FetchedCounts(key, eqWithValues)
              }
            case _ =>
              Nil
          }
        }
      case None =>
        Future.successful(Nil)
    }
  }

  // for query exact qualifier
  override def get(policy: Counter, queries: Seq[(ExactKeyTrait, Seq[ExactQualifier])])
                  (implicit ex: ExecutionContext): Future[Seq[FetchedCounts]] = {
    val futures = {
      for {
        (key, eqs) <- queries
      } yield {
//        println(s"$key $eqs")
        getInner(policy, key, eqs)
      }
    }
    Future.sequence(futures).map(_.flatten)
  }

  override def getBlobValue(policy: Counter, blobId: String): Option[String] = {
    throw new RuntimeException("unsupported getBlobValue operation")
  }

  override def insertBlobValue(policy: Counter, keys: Seq[BlobExactKey]): Seq[Boolean] = {
    throw new RuntimeException("unsupported insertBlobValue operation")
  }

  private def existsLabel(policy: Counter, useCache: Boolean = true): Boolean = {
    val action = policy.action
    val counterLabelName = action + labelPostfix

    Label.findByName(counterLabelName, useCache).nonEmpty
  }

  override def prepare(policy: Counter): Unit = {
    val service = policy.service
    val action = policy.action

    val graphLabel = Label.findByName(action)
    if (graphLabel.isEmpty) {
      throw new Exception(s"label not found. $service.$action")
    }

    if (!existsLabel(policy, useCache = false)) {
      val label = Label.findByName(action, useCache = false).get

      val counterLabelName = action + labelPostfix
      val defaultJson =
        s"""
           |{
           |  "label": "$counterLabelName",
           |  "srcServiceName": "$service",
           |  "srcColumnName": "${label.tgtColumnName}",
           |  "srcColumnType": "${label.tgtColumnType}",
           |  "tgtServiceName": "$SERVICE_NAME",
           |  "tgtColumnName": "$COLUMN_NAME",
           |  "tgtColumnType": "string",
           |  "indices": [
           |    {"name": "time", "propNames": ["_to", "time_unit", "time_value"]}
           |  ],
           |  "props": [
           |    {"name": "time_unit", "dataType": "string", "defaultValue": ""},
           |    {"name": "time_value", "dataType": "long", "defaultValue": 0}
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

  // for range query
  override def get(policy: Counter, items: Seq[String], timeRange: Seq[(TimedQualifier, TimedQualifier)])
                  (implicit ec: ExecutionContext): Future[Seq[FetchedCounts]] = {
    throw new NotImplementedError("Not implemented")
  }
}
