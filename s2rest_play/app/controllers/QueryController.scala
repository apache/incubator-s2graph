package controllers


import com.kakao.s2graph.core._
import com.kakao.s2graph.core.rest.RestHandler
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Controller, Request}

import scala.language.postfixOps

object QueryController extends Controller with JSONParser {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private val rest: RestHandler = com.kakao.s2graph.rest.Global.s2rest

  def delegate(request: Request[JsValue]) =
    rest.doPost(request.uri, request.body).body.map { js =>
      jsonResponse(js, "result_size" -> rest.calcSize(js).toString)
    } recoverWith ApplicationController.requestFallback(request.body)

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
      rest.checkEdges(params).body.map { js =>
        jsonResponse(js, "result_size" -> rest.calcSize(js).toString)
      } recoverWith ApplicationController.requestFallback(request.body)
    }

  def getVertices() = withHeaderAsync(jsonParser)(delegate)
}
