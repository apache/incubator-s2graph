package controllers


import actors.QueueActor
import com.kakao.s2graph.core.rest.RequestParser
import com.kakao.s2graph.core.utils.logger
import com.kakao.s2graph.core.{ExceptionHandler, Graph, GraphExceptions}
import config.Config
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Controller, Result}

import scala.concurrent.Future

object VertexController extends Controller {
  private val s2: Graph = com.kakao.s2graph.rest.Global.s2graph
  private val requestParser: RequestParser = com.kakao.s2graph.rest.Global.s2parser

  import ExceptionHandler._
  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._

  def tryMutates(jsValue: JsValue, operation: String, serviceNameOpt: Option[String] = None, columnNameOpt: Option[String] = None, withWait: Boolean = false): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)
    else {
      try {
        val vertices = requestParser.toVertices(jsValue, operation, serviceNameOpt, columnNameOpt)

        for (vertex <- vertices) {
          if (vertex.isAsync)
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC_ASYNC, vertex, None))
          else
            ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC, vertex, None))
        }

        //FIXME:
        val verticesToStore = vertices.filterNot(v => v.isAsync)

        if (withWait) {
          val rets = s2.mutateVertices(verticesToStore, withWait = true)
          rets.map(Json.toJson(_)).map(jsonResponse(_))
        } else {
          val rets = verticesToStore.map { vertex => QueueActor.router ! vertex; true }
          Future.successful(jsonResponse(Json.toJson(rets)))
        }
      } catch {
        case e: GraphExceptions.JsonParseException => Future.successful(BadRequest(s"e"))
        case e: Exception =>
          logger.error(s"[Failed] tryMutates", e)
          Future.successful(InternalServerError(s"${e.getStackTrace}"))
      }
    }
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert")
  }

  def insertsWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert", withWait = true)
  }

  def insertsSimple(serviceName: String, columnName: String) = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert", Some(serviceName), Some(columnName))
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete")
  }

  def deletesWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete", withWait = true)
  }

  def deletesSimple(serviceName: String, columnName: String) = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete", Some(serviceName), Some(columnName))
  }

  def deletesAll() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "deleteAll")
  }

  def deletesAllSimple(serviceName: String, columnName: String) = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "deleteAll", Some(serviceName), Some(columnName))
  }

}
