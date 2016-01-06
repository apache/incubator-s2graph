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

  //  private def toSimpleMap(map: Map[String, Seq[String]]): Map[String, String] = {
  //    for {
  //      (k, vs) <- map
  //      headVal <- vs.headOption
  //    } yield {
  //      k -> headVal
  //    }
  //  }

  //  private def buildRequest(request: Request[AnyContent], bucket: Bucket, uuid: String): Future[Result] = {
  //    val jsonBody = makeRequestJson(request.body.asJson, bucket, uuid)
  //
  //    val url = bucket.apiPath
  //    val headers = request.headers.toSimpleMap.toSeq
  //    val verb = bucket.httpVerb.toUpperCase
  //    val qs = toSimpleMap(request.queryString).toSeq
  //
  //    val ws = WS.url(url)
  //      .withMethod(verb)
  //      .withBody(jsonBody)
  //      .withHeaders(headers: _*)
  //      .withQueryString(qs: _*)
  //
  //    ws.stream().map {
  //      case (proxyResponse, proxyBody) =>
  //        Result(ResponseHeader(proxyResponse.status, proxyResponse.headers.mapValues(_.toList.head)), proxyBody).withHeaders(impressionKey -> bucket.impressionId)
  //    }
  //  }
}
