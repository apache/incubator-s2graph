package controllers


import com.daumkakao.s2graph.rest.config.{Instrumented, Config}
import com.daumkakao.s2graph.core.{Graph, Vertex, KGraphExceptions}
import play.api.Logger
import play.api.libs.json.JsValue
import play.api.mvc.{ Controller, Result }
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

object VertexController extends Controller with Instrumented  with RequestParser  {

  import controllers.ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._
  private def tryMutates(jsValue: JsValue, operation: String, serviceNameOpt: Option[String] = None, columnNameOpt: Option[String] = None): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)

    try {
      val vertices = toVertices(jsValue, operation, serviceNameOpt, columnNameOpt)
      getOrElseUpdateMetric("incommingVertices")(metricRegistry.counter("incommingVertices")).inc(vertices.size)
      Graph.mutateVertices(vertices).map { rets =>
        Ok(s"$rets").as(QueryController.applicationJsonHeader)
      }
    } catch {
      case e: KGraphExceptions.JsonParseException => Future.successful(BadRequest(s"e"))
      case e: Throwable =>
        Logger.error(s"[Failed] tryMutates", e)
        Future.successful(InternalServerError(s"${e.getStackTraceString}"))
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