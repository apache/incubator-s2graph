package controllers


import com.kakao.s2graph.core.mysqls.Experiment
import com.kakao.s2graph.core.rest.RestCaller
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

object ExperimentController extends Controller {
  private val rest: RestCaller = com.kakao.s2graph.rest.Global.s2rest

  import ApplicationController._

  def experiment(accessToken: String, experimentName: String, uuid: String) = withHeaderAsync(parse.anyContent) { request =>
    val body = request.body.asJson.get

    val res = rest.experiment(body, accessToken, experimentName, uuid)
    res.map { case (js, impId) =>
      jsonResponse(js, Experiment.impressionKey -> impId, "result_size" -> rest.calcSize(js).toString)
    } recoverWith ApplicationController.requestFallback(body)
  }
}
