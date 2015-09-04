package controllers

import actors.QueueActor
import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls.Label
import com.daumkakao.s2graph.logger
import config.Config
import play.api.Logger
import play.api.libs.json._
import play.api.mvc.{Controller, Result}

import scala.concurrent.Future

object EdgeController extends Controller with RequestParser {

  import ExceptionHandler._
  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._

  private val maxLength = 1024 * 1024 * 16

  def tryMutates(jsValue: JsValue, operation: String): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)
    else {
      try {
        logger.debug(s"$jsValue")
        val edges = toEdges(jsValue, operation)
        for {edge <- edges} {
          if (edge.isAsync) {
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC_ASYNC, edge, None))
          } else {
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC, edge, None))
          }
        }

        val edgesToStore = edges.filterNot(e => e.isAsync)

        val rets = for {
          element <- edgesToStore
        } yield {
          QueueActor.router ! element
          true

        }
        Future.successful(jsonResponse(Json.toJson(rets)))
      } catch {
        case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
        case e: Throwable =>
          logger.error(s"mutateAndPublish: $e", e)
          Future.successful(InternalServerError(s"${e.getStackTrace}"))
      }
    }
  }

  //  private[controllers] def aggregateElements(elements: Seq[GraphElement], originalString: Seq[Option[String]]): Future[Seq[Boolean]] = {
  //    elements.foreach {
  //
  //    }
  //    KafkaAggregatorActor.enqueue(Protocol.elementToKafkaMessage(Config.KAFKA_LOG_TOPIC, element, originalString))
  //    Graph.mutateElements(elements)
  //  }
  def mutateAndPublish(str: String): Future[Result] = {
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
      val rets = for {
        element <- elementsToStore
      } yield {
          logger.debug(s"sending actor: $element")
          QueueActor.router ! element
          true
        }

      Future.successful(jsonResponse(Json.toJson(rets)))

    } catch {
      case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
      case e: Throwable =>
        logger.error(s"mutateAndPublish: $e", e)
        Future.successful(InternalServerError(s"${e.getStackTrace}"))
    }
  }

  def mutateBulk() = withHeaderAsync(parse.text) { request =>
    mutateAndPublish(request.body)
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert")
  }

  def insertsBulk() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insertBulk")
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete")
  }

  def updates() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "update")
  }

  def increments() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "increment")
  }

  def incrementCounts() = withHeaderAsync(jsonParser) { request =>
    val jsValue = request.body
    val edges = toEdges(jsValue, "incrementCount")
    Edge.incrementCounts(edges).map { results =>
      val json = results.map { case (isSuccess, resultCount) =>
        Json.obj("success" -> isSuccess, "result" -> resultCount)
      }

      jsonResponse(Json.toJson(json))
    }
  }

  def deleteAll() = withHeaderAsync(jsonParser) { request =>
    deleteAllInner(request.body)
  }

  def deleteAllInner(jsValue: JsValue) = {
    val deleteResults = Future.sequence(jsValue.as[Seq[JsValue]] map { json =>
      val labelName = (json \ "label").as[String]
      val labels = Label.findByName(labelName).map { l => Seq(l) }.getOrElse(Nil)
      val direction = (json \ "direction").asOpt[String].getOrElse("out")

      val ids = (json \ "ids").asOpt[List[JsValue]].getOrElse(Nil)
      val ts = (json \ "timestamp").asOpt[Long]
      val vertices = toVertices(labelName, direction, ids)

      Graph.deleteVerticesAllAsync(vertices.toList, labels, GraphUtil.directions(direction), ts)
    })

    deleteResults.map { rst =>
      logger.debug(s"deleteAllInner: $rst")
      Ok(s"deleted... ${rst.toString()}")
    }
  }
}
