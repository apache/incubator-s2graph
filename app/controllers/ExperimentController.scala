package controllers


import java.net.URL

import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.logger
import play.api.Play.current
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import play.api.libs.ws.WS
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Created by shon on 8/5/15.
 */
object ExperimentController extends Controller with RequestParser {
  val impressionKey = "S2-Impression-Id"

  import ApplicationController._

  def experiment(accessToken: String, experimentName: String, uuid: String) = withHeaderAsync(parse.anyContent) { request =>
    val bucketOpt = for {
      service <- Service.findByAccessToken(accessToken)
      experiment <- Experiment.findBy(service.id.get, experimentName)
      bucket <- experiment.findBucket(uuid)
    } yield bucket
    bucketOpt match {
      case None => Future.successful(NotFound("bucket is not found."))
      case Some(bucket) =>
        try {
          if (bucket.isGraphQuery) buildRequestInner(request, bucket, uuid)
          else buildRequest(request, bucket, uuid)
        } catch {
          case e: Exception =>
            logger.error(e.toString())
            Future.successful(BadRequest(s"wrong or missing template parameter: ${e.getMessage}"))
        }
    }
  }

  def makeRequestJson(requestKeyJsonOpt: Option[JsValue], bucket: Bucket, uuid: String): JsValue = {
    var body = bucket.requestBody.replace("#uuid", uuid)

    for {
      requestKeyJson <- requestKeyJsonOpt
      jsObj <- requestKeyJson.asOpt[JsObject]
      (key, value) <- jsObj.fieldSet
    } {
      val replacement = value match {
        case JsString(s) => s
        case _ => value.toString
      }
      body = body.replace(key, replacement)
    }

    Json.parse(body)
  }

  private def buildRequestInner(request: Request[AnyContent], bucket: Bucket, uuid: String): Future[Result] = {
    val jsonBody = makeRequestJson(request.body.asJson, bucket, uuid)

    val url = new URL(bucket.apiPath)
    val response = url.getPath() match {
      case "/graphs/getEdges" => controllers.QueryController.getEdgesInner(jsonBody)
      case "/graphs/getEdges/grouped" => controllers.QueryController.getEdgesWithGroupingInner(jsonBody)
      case "/graphs/getEdgesExcluded" => controllers.QueryController.getEdgesExcludedInner(jsonBody)
      case "/graphs/getEdgesExcluded/grouped" => controllers.QueryController.getEdgesExcludedWithGroupingInner(jsonBody)
      case "/graphs/checkEdges" => controllers.QueryController.checkEdgesInner(jsonBody)
      case "/graphs/getEdgesGrouped" => controllers.QueryController.getEdgesGroupedInner(jsonBody)
      case "/graphs/getEdgesGroupedExcluded" => controllers.QueryController.getEdgesGroupedExcludedInner(jsonBody)
      case "/graphs/getEdgesGroupedExcludedFormatted" => controllers.QueryController.getEdgesGroupedExcludedFormattedInner(jsonBody)
    }

    response.map { r => r.withHeaders(impressionKey -> bucket.impressionId) }
  }

  private def toSimpleMap(map: Map[String, Seq[String]]): Map[String, String] = {
    for {
      (k, vs) <- map
      headVal <- vs.headOption
    } yield {
      k -> headVal
    }
  }

  private def buildRequest(request: Request[AnyContent], bucket: Bucket, uuid: String): Future[Result] = {
    val jsonBody = makeRequestJson(request.body.asJson, bucket, uuid)

    val url = bucket.apiPath
    val headers = request.headers.toSimpleMap.toSeq
    val verb = bucket.httpVerb.toUpperCase
    val qs = toSimpleMap(request.queryString).toSeq

    val ws = WS.url(url)
      .withMethod(verb)
      .withBody(jsonBody)
      .withHeaders(headers: _*)
      .withQueryString(qs: _*)

    ws.stream().map {
      case (proxyResponse, proxyBody) =>
        Result(ResponseHeader(proxyResponse.status, proxyResponse.headers.mapValues(_.toList.head)), proxyBody).withHeaders(impressionKey -> bucket.impressionId)
    }
  }
}
