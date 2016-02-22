package controllers


import com.kakao.s2graph.core.mysqls.Experiment
import com.kakao.s2graph.core.rest.RestHandler
import com.kakao.s2graph.core.utils.logger
import play.api.mvc._
import scala.concurrent.ExecutionContext.Implicits.global

object ExperimentController extends Controller {
  private val rest: RestHandler = com.kakao.s2graph.rest.Global.s2rest

  import ApplicationController._

  def experiment(accessToken: String, experimentName: String, uuid: String) = withHeaderAsync(jsonText) { request =>
    val body = request.body

    val res = rest.doPost(request.uri, body, request.headers.get(Experiment.impressionKey))
    res.body.map { case js =>
      val headers = res.headers :+ ("result_size" -> rest.calcSize(js).toString)
      jsonResponse(js, headers: _*)
    } recoverWith ApplicationController.requestFallback(body)
  }
}
