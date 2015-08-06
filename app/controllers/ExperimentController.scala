package controllers


import com.daumkakao.s2graph.core.mysqls._
import play.api.Play.current
import play.api.libs.ws.WS
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by shon on 8/5/15.
 */
object ExperimentController extends Controller {
  val impressionKey = "S2-Impression-Id"

  def experiment(serviceName: String, experimentKey: String, uuid: String) = Action.async { request =>
    Experiment.find(serviceName, experimentKey) match {
      case None => throw new RuntimeException("not found experiment")
      case Some(experiment) =>
        experiment.findBucket(uuid) match {
          case None => throw new RuntimeException("bucket is not found")
          case Some(bucket) => buildRequest(request, uuid, bucket)
        }
    }
  }

  private def buildRequest(request: Request[AnyContent], uuid: String, bucket: Bucket) = {
    val url = bucket.apiPath
    val headers = request.headers.toSimpleMap.toSeq
    val body = bucket.requestBody.replace(bucket.uuidPlaceHolder, uuid)
    val verb = bucket.httpVerb.toUpperCase
    val qs = Bucket.toSimpleMap(request.queryString).toSeq

    val ws = WS.url(url)
      .withMethod(verb)
      .withBody(body)
      .withHeaders(headers: _*)
      .withQueryString(qs: _*)

    ws.stream().map {
      case (proxyResponse, proxyBody) =>
        Result(ResponseHeader(proxyResponse.status, proxyResponse.headers.mapValues(_.toList.head)), proxyBody)
    }
  }
}
