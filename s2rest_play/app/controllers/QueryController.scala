package controllers

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.Experiment
import com.kakao.s2graph.core.rest.RestHandler
import play.api.libs.json.{Json}
import play.api.mvc._

import scala.language.postfixOps

object QueryController extends Controller with JSONParser {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private val rest: RestHandler = com.kakao.s2graph.rest.Global.s2rest

  def delegate(request: Request[String]) = {
    rest.doPost(request.uri, request.body, request.headers.get(Experiment.impressionKey)).body.map {
      js =>
        jsonResponse(js, "result_size" -> rest.calcSize(js).toString)
    } recoverWith ApplicationController.requestFallback(request.body)
  }

  def getEdges() = withHeaderAsync(jsonText)(delegate)

  def getEdgesWithGrouping() = withHeaderAsync(jsonText)(delegate)

  def getEdgesExcluded() = withHeaderAsync(jsonText)(delegate)

  def getEdgesExcludedWithGrouping() = withHeaderAsync(jsonText)(delegate)

  def checkEdges() = withHeaderAsync(jsonText)(delegate)

  def getEdgesGrouped() = withHeaderAsync(jsonText)(delegate)

  def getEdgesGroupedExcluded() = withHeaderAsync(jsonText)(delegate)

  def getEdgesGroupedExcludedFormatted() = withHeaderAsync(jsonText)(delegate)

  def getEdge(srcId: String, tgtId: String, labelName: String, direction: String) =
    withHeaderAsync(jsonText) {
      request =>
        val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
        rest.checkEdges(params).body.map {
          js =>
            jsonResponse(js, "result_size" -> rest.calcSize(js).toString)
        } recoverWith ApplicationController.requestFallback(request.body)
    }

  def getVertices() = withHeaderAsync(jsonText)(delegate)
}
