package controllers

import com.daumkakao.s2graph.core.mysqls.Label
import com.daumkakao.s2graph.core._
import config.Config
import play.api.Logger
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Controller, Result}
import scala.concurrent.Future

object EdgeController extends Controller with RequestParser {

  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._
  import ExceptionHandler._

  private val maxLength = 1024 * 1024 * 16

  def tryMutates(jsValue: JsValue, operation: String): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)
    else {
      try {
        Logger.debug(s"$jsValue")
        val edges = toEdges(jsValue, operation)
        for {edge <- edges} {
          if (edge.isAsync) {
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC_ASYNC, edge, None))
          } else {
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC, edge, None))
          }
        }

        val edgesToStore = edges.filterNot(e => e.isAsync)
        //FIXME:
        Graph.mutateEdges(edgesToStore).map { rets =>
          Ok(s"${Json.toJson(rets)}").as(QueryController.applicationJsonHeader)
        }

      } catch {
        case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
        case e: Throwable =>
          play.api.Logger.error(s"mutateAndPublish: $e", e)
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

    Logger.debug(s"$str")
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
      Graph.mutateElements(elementsToStore).map { rets =>
        Ok(s"${Json.toJson(rets)}").as(QueryController.applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
      case e: Throwable =>
        play.api.Logger.error(s"mutateAndPublish: $e", e)
        Future.successful(InternalServerError(s"${e.getStackTrace}"))
    }
  }

  def mutateBulk() = withHeaderAsync(parse.text) { request =>
    mutateAndPublish(request.body)
  }


  def inserts() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "insert")
  }


  def insertsBulk() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "insertBulk")
  }

  def deletes() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "delete")
  }

  def updates() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "update")
  }

  def increments() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "increment")
  }

  def incrementCounts() = withHeaderAsync(parse.json) { request =>
    val jsValue = request.body
    val edges = toEdges(jsValue, "incrementCount")
    Edge.incrementCounts(edges).map { results =>
      val json = results.map { case (isSuccess, resultCount) =>
        Json.obj("success" -> isSuccess, "result" -> resultCount)
      }
      Ok(Json.toJson(json))
    }
  }

  def deleteAll() = withHeaderAsync(parse.json) { request =>
    val deleteResults = Future.sequence(request.body.as[Seq[JsValue]] map { json =>
      val labelName = (json \ "label").as[String]
      val labels = Label.findByName(labelName).map { l => Seq(l) }.getOrElse(Nil)
      val direction = (json \ "direction").asOpt[String].getOrElse("out")

      val ids = (json \ "ids").asOpt[List[JsValue]].getOrElse(Nil)
      val ts = (json \ "timestamp").asOpt[Long]
      val vertices = toVertices(labelName, direction, ids)

      Graph.deleteVerticesAllAsync(vertices.toList, labels, GraphUtil.directions(direction), ts)
    })

    deleteResults.map { rst =>
      Ok(s"deleted... ${rst.toString()}")
    }
  }
}
