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

  def getEdges() = withHeaderAsync(parse.tolerantText)(delegate)

  def getEdgesWithGrouping() = withHeaderAsync(parse.tolerantText)(delegate)

  def getEdgesExcluded() = withHeaderAsync(parse.tolerantText)(delegate)

  def getEdgesExcludedWithGrouping() = withHeaderAsync(parse.tolerantText)(delegate)

  def checkEdges() = withHeaderAsync(parse.tolerantText)(delegate)

  def getEdgesGrouped() = withHeaderAsync(parse.tolerantText)(delegate)

  def getEdgesGroupedExcluded() = withHeaderAsync(parse.tolerantText)(delegate)

  def getEdgesGroupedExcludedFormatted() = withHeaderAsync(parse.tolerantText)(delegate)

  def getEdge(srcId: String, tgtId: String, labelName: String, direction: String) =
    withHeaderAsync(parse.tolerantText) {
      request =>
        val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
        rest.checkEdges(params).body.map {
          js =>
            jsonResponse(js, "result_size" -> rest.calcSize(js).toString)
        } recoverWith ApplicationController.requestFallback(request.body)
    }

  def getVertices() = withHeaderAsync(parse.tolerantText)(delegate)
}
