package controllers


import actors.QueueActor
import com.daumkakao.s2graph.core.{ExceptionHandler, KGraphExceptions}
import com.daumkakao.s2graph.logger
import config.Config
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Controller, Result}

import scala.concurrent.Future

object VertexController extends Controller with RequestParser  {

  import ExceptionHandler._
  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._
  private def tryMutates(jsValue: JsValue, operation: String, serviceNameOpt: Option[String] = None, columnNameOpt: Option[String] = None): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)

    try {
      val vertices = toVertices(jsValue, operation, serviceNameOpt, columnNameOpt)

      for { vertex <- vertices } {
        if (vertex.isAsync) {
          ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC_ASYNC, vertex, None))
        } else {
          ExceptionHandler.enqueue(toKafkaMessage(Config.KAFKA_LOG_TOPIC, vertex, None))
        }
      }
      //FIXME:
      val verticesToStore = vertices.filterNot(v => v.isAsync)
      val rets = for {
        element <- verticesToStore
      } yield {
          QueueActor.router ! element
          true
        }

      Future.successful(jsonResponse(Json.toJson(rets)))

    } catch {
      case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"e"))
      case e: Throwable =>
        logger.error(s"[Failed] tryMutates", e)
        Future.successful(InternalServerError(s"${e.getStackTrace}"))
    }
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert")
  }

  def insertsSimple(serviceName: String, columnName: String) = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert", Some(serviceName), Some(columnName))
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete")
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
