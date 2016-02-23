package controllers


import com.kakao.s2graph.core.rest.RestHandler
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

object ExperimentController extends Controller {
  private val rest: RestHandler = com.kakao.s2graph.rest.Global.s2rest

  import ApplicationController._

  def experiment(accessToken: String, experimentName: String, uuid: String) = withHeaderAsync(parse.anyContent) { request =>
    val body = request.body.asJson.get
    val res = rest.doPost(request.uri, body)

    res.body.map { case js =>
      val headers = res.headers :+ ("result_size" -> rest.calcSize(js).toString)
      jsonResponse(js, headers: _*)
    } recoverWith ApplicationController.requestFallback(body)
  }
}
