package controllers

import actors.QueueActor
import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{LabelMeta, Label}
import com.kakao.s2graph.core.rest.RequestParser
import com.kakao.s2graph.core.types.LabelWithDirection
import com.kakao.s2graph.core.utils.logger
import config.Config
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json._
import play.api.mvc.{Controller, Result}

import scala.collection.Seq
import scala.concurrent.Future
import scala.util.{Failure, Success}

object EdgeController extends Controller {

  import ExceptionHandler._
  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._

  private val s2: Graph = com.kakao.s2graph.rest.Global.s2graph
  private val requestParser: RequestParser = com.kakao.s2graph.rest.Global.s2parser

  def tryMutates(jsValue: JsValue, operation: String, withWait: Boolean = false): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)

    else {
      try {
        logger.debug(s"$jsValue")
        val edges = requestParser.toEdges(jsValue, operation)
        for (edge <- edges) {
          if (edge.isAsync)
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC_ASYNC, edge, None))
          else
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC, edge, None))
        }

        val edgesToStore = edges.filterNot(e => e.isAsync)

        if (withWait) {
          val rets = s2.mutateEdges(edgesToStore, withWait = true)
          rets.map(Json.toJson(_)).map(jsonResponse(_))
        } else {
          val rets = edgesToStore.map { edge => QueueActor.router ! edge; true }
          Future.successful(jsonResponse(Json.toJson(rets)))
        }
      } catch {
        case e: GraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
        case e: Exception =>
          logger.error(s"mutateAndPublish: $e", e)
          Future.successful(InternalServerError(s"${e.getStackTrace}"))
      }
    }
  }

  def mutateAndPublish(str: String, withWait: Boolean = false): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)

    logger.debug(s"$str")
    val edgeStrs = str.split("\\n")

    var vertexCnt = 0L
    var edgeCnt = 0L
    try {
      val elements =
        for (edgeStr <- edgeStrs; str <- GraphUtil.parseString(edgeStr); element <- Graph.toGraphElement(str)) yield {
          element match {
            case v: Vertex => vertexCnt += 1
            case e: Edge => edgeCnt += 1
          }
          if (element.isAsync) {
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC_ASYNC, element, Some(str)))
          } else {
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC, element, Some(str)))
          }
          element
        }

      //FIXME:
      val elementsToStore = elements.filterNot(e => e.isAsync)
      if (withWait) {
        val rets = s2.mutateElements(elementsToStore, withWait)
        rets.map(Json.toJson(_)).map(jsonResponse(_))
      } else {
        val rets = elementsToStore.map { element => QueueActor.router ! element; true }
        Future.successful(jsonResponse(Json.toJson(rets)))
      }


    } catch {
      case e: GraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
      case e: Throwable =>
        logger.error(s"mutateAndPublish: $e", e)
        Future.successful(InternalServerError(s"${e.getStackTrace}"))
    }
  }

  def mutateBulk() = withHeaderAsync(parse.text) { request =>
    mutateAndPublish(request.body, withWait = false)
  }

  def mutateBulkWithWait() = withHeaderAsync(parse.text) { request =>
    mutateAndPublish(request.body, withWait = true)
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert")
  }

  def insertsWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert", withWait = true)
  }

  def insertsBulk() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insertBulk")
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete")
  }

  def deletesWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete", withWait = true)
  }

  def updates() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "update")
  }

  def updatesWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "update", withWait = true)
  }

  def increments() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "increment")
  }

  def incrementCounts() = withHeaderAsync(jsonParser) { request =>
    val jsValue = request.body
    val edges = requestParser.toEdges(jsValue, "incrementCount")
    s2.incrementCounts(edges).map { results =>
      val json = results.map { case (isSuccess, resultCount) =>
        Json.obj("success" -> isSuccess, "result" -> resultCount)
      }

      jsonResponse(Json.toJson(json))
    }
  }

  def deleteAll() = withHeaderAsync(jsonParser) { request =>
    deleteAllInner(request.body, withWait = false)
  }

  def deleteAllInner(jsValue: JsValue, withWait: Boolean) = {
    val deleteResults = Future.sequence(jsValue.as[Seq[JsValue]] map { json =>

      val labelName = (json \ "label").as[String]
      val labels = Label.findByName(labelName).map { l => Seq(l) }.getOrElse(Nil)
      val direction = (json \ "direction").asOpt[String].getOrElse("out")

      val ids = (json \ "ids").asOpt[List[JsValue]].getOrElse(Nil)
      val ts = (json \ "timestamp").asOpt[Long].getOrElse(System.currentTimeMillis())
      val vertices = requestParser.toVertices(labelName, direction, ids)

      /** logging for delete all request */

      val kafkaMessages = for {
        id <- ids
        label <- labels
      } yield {
          val tsv = Seq(ts, "deleteAll", "e", id, id, label.label, "{}", direction).mkString("\t")
          val topic = if (label.isAsync) Config.KAFKA_LOG_TOPIC_ASYNC else Config.KAFKA_LOG_TOPIC
          val kafkaMsg = KafkaMessage(new ProducerRecord[Key, Val](topic, null, tsv))
          kafkaMsg
        }
      ExceptionHandler.enqueues(kafkaMessages)

      val future = s2.deleteAllAdjacentEdges(vertices.toList, labels, GraphUtil.directions(direction), ts)
      future.onFailure { case ex: Exception =>
        logger.error(s"[Error]: deleteAllInner failed.", ex)
        val kafkaMessages = for {
          id <- ids
          label <- labels
        } yield {
            val tsv = Seq(ts, "deleteAll", "e", id, id, label.label, "{}", direction).mkString("\t")
            val topic = failTopic
            val kafkaMsg = KafkaMessage(new ProducerRecord[Key, Val](topic, null, tsv))
            kafkaMsg
          }
        ExceptionHandler.enqueues(kafkaMessages)
        throw ex
      }
      if (withWait) {
        future.map { ret =>
          if (!ret) {
            logger.error(s"[Error]: deleteAllInner failed.")
            val kafkaMessages = for {
              id <- ids
              label <- labels
            } yield {
                val tsv = Seq(ts, "deleteAll", "e", id, id, label.label, "{}", direction).mkString("\t")
                val topic = failTopic
                val kafkaMsg = KafkaMessage(new ProducerRecord[Key, Val](topic, null, tsv))
                kafkaMsg
              }
            ExceptionHandler.enqueues(kafkaMessages)
            false
          } else {
            true
          }
        }
      } else {
        Future.successful(true)
      }
    })

    deleteResults.map { rst =>
      logger.debug(s"deleteAllInner: $rst")
      Ok(s"deleted... ${rst.toString()}")
    }
  }
}
