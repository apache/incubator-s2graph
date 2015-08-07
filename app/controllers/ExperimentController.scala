package controllers


import java.net.URL
import com.daumkakao.s2graph.core.mysqls._
import play.api.Play.current
import play.api.libs.json.Json
import play.api.libs.ws.WS
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by shon on 8/5/15.
 */
object ExperimentController extends Controller with RequestParser {
  val impressionKey = "S2-Impression-Id"

  def experiment(accessToken: String, experimentName: String, uuid: String) = Action.async { request =>
    val bucketOpt = for {
      service <- Service.findByAccessToken(accessToken)
      experiment <- Experiment.findBy(service.id.get, experimentName)
      bucket <- experiment.findBucket(uuid)
    } yield bucket
    bucketOpt match {
      case None => Future.successful(NotFound("bucket is not found."))
      case Some(bucket) =>
        if (bucket.isGraphQuery) buildRequestInner(request, uuid, bucket)
        else buildRequest(request, uuid, bucket)
    }
  }

  private def buildRequestInner(request: Request[AnyContent], uuid: String, bucket: Bucket): Future[Result] = {
    val jsonBody = Json.parse(bucket.requestBody.replace(bucket.uuidPlaceHolder, uuid))
    val url = new URL(bucket.apiPath)

    val future = url.getPath() match {
      case "/graphs/getEdges" => controllers.QueryController.getEdgesInner(jsonBody)
      case "/graphs/getEdges/grouped" => controllers.QueryController.getEdgesWithGroupingInner(jsonBody)
      case "/graphs/getEdgesExcluded" => controllers.QueryController.getEdgesExcludedInner(jsonBody)
      case "/graphs/getEdgesExcluded/grouped" => controllers.QueryController.getEdgesExcludedWithGroupingInner(jsonBody)
      case "/graphs/checkEdges" =>  controllers.QueryController.checkEdgesInner(jsonBody)
      case "/graphs/getEdgesGrouped" => controllers.QueryController.getEdgesGroupedInner(jsonBody)
      case "/graphs/getEdgesGroupedExcluded" => controllers.QueryController.getEdgesGroupedExcludedInner(jsonBody)
      case "/graphs/getEdgesGroupedExcludedFormatted" => controllers.QueryController.getEdgesGroupedExcludedFormattedInner(jsonBody)
    }
    future.map { r => r.withHeaders(impressionKey -> bucket.impressionId) }
  }
  private def buildRequest(request: Request[AnyContent], uuid: String, bucket: Bucket): Future[Result] = {
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
        Result(ResponseHeader(proxyResponse.status, proxyResponse.headers.mapValues(_.toList.head)), proxyBody).withHeaders(impressionKey -> bucket.impressionId)
    }
  }
}
