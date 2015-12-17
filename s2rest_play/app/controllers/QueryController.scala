package controllers


import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.rest.RestCaller
import com.kakao.s2graph.core.utils.logger
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Controller, Request, Result}

import scala.concurrent._
import scala.language.postfixOps

object QueryController extends Controller with JSONParser {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private val rest: RestCaller = com.kakao.s2graph.rest.Global.s2rest

  private def badQueryExceptionResults(ex: Exception) = Future.successful(BadRequest(Json.obj("message" -> ex.getMessage)).as(applicationJsonHeader))

  private def errorResults = Future.successful(Ok(PostProcess.timeoutResults).as(applicationJsonHeader))

  def fallback(body: JsValue): PartialFunction[Throwable, Future[Result]] = {
    case e: BadQueryException =>
      logger.error(s"{$body}, $e", e)
      badQueryExceptionResults(e)
    case e: Exception =>
      logger.error(s"${body}, ${e.getMessage}", e)
      errorResults
  }

  def delegate(request: Request[JsValue]) =
    rest.uriMatch(request.uri, request.body).map { js =>
      Ok(js)
    } recoverWith fallback(request.body)

  def getEdges() = withHeaderAsync(jsonParser)(delegate)

  def getEdgesWithGrouping() = withHeaderAsync(jsonParser)(delegate)

  def getEdgesExcluded() = withHeaderAsync(jsonParser)(delegate)

  def getEdgesExcludedWithGrouping() = withHeaderAsync(jsonParser)(delegate)

  def checkEdges() = withHeaderAsync(jsonParser)(delegate)

  def getEdgesGrouped() = withHeaderAsync(jsonParser)(delegate)

  def getEdgesGroupedExcluded() = withHeaderAsync(jsonParser)(delegate)

  def getEdgesGroupedExcludedFormatted() = withHeaderAsync(jsonParser)(delegate)

  def getEdge(srcId: String, tgtId: String, labelName: String, direction: String) =
    withHeaderAsync(jsonParser) { request =>
      val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
      rest.checkEdges(params).map { js =>
        Ok(js)
      } recoverWith fallback(request.body)
    }

  def getVertices() = withHeaderAsync(jsonParser)(delegate)
}
