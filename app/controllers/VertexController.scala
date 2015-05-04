package controllers

import javax.ws.rs.PathParam

import com.daumkakao.s2graph.rest.config.{Instrumented, Config}
import com.daumkakao.s2graph.core.{ Vertex, Logger, KGraphExceptions }
import com.wordnik.swagger.annotations._
import play.api.libs.json.JsValue
import play.api.mvc.{ Controller, Result }
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object VertexController extends Controller with Instrumented  with RequestParser  {

  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._
  private def tryMutates(jsValue: JsValue, operation: String, serviceNameOpt: Option[String] = None, columnNameOpt: Option[String] = None): Future[Result] = {
    Future {
      if (!Config.IS_WRITE_SERVER) Unauthorized
      
      val vertices = new ListBuffer[Vertex]
      try {
        vertices ++= toVertices(jsValue, operation, serviceNameOpt, columnNameOpt)

        for (vertex <- vertices) {
          try {
            Logger.debug(s"$vertex")
            EdgeController.aggregateElement(vertex, None)
          } catch {
            case e: Throwable => Logger.error(s"tryMutates: $vertex, $e", e)
          }
        }

        getOrElseUpdateMetric("incommingVertices")(metricRegistry.counter("incommingVertices")).inc(vertices.size)
        Ok(s"${vertices.size} $operation success\n")
      } catch {
        case e: KGraphExceptions.JsonParseException => BadRequest(s"e")
        case e: Throwable =>
          Logger.error(s"[Failed] tryMutates", e)
          InternalServerError(s"${e.getStackTraceString}")
      }
    }
  }

  def inserts() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "insert")
  }

  def insertsSimple(serviceName: String, columnName: String) = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "insert", Some(serviceName), Some(columnName))
  }

  def deletes() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "delete")
  }

  def deletesSimple(serviceName: String, columnName: String) = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "delete", Some(serviceName), Some(columnName))
  }

  def deletesAll() = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "deleteAll")
  }

  def deletesAllSimple(serviceName: String, columnName: String) = withHeaderAsync(parse.json) { request =>
    tryMutates(request.body, "deleteAll", Some(serviceName), Some(columnName))
  }
}