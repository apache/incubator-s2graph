package org.apache.s2graph.rest.play.controllers

import org.apache.s2graph.core.mysqls.Experiment
import org.apache.s2graph.core.rest.RestHandler
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

object ExperimentController extends Controller {
  private val rest: RestHandler = org.apache.s2graph.rest.play.Global.s2rest

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
