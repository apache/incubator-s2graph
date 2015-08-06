package controllers


import com.daumkakao.s2graph.core.mysqls._
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.Json
import play.api.libs.ws.{WS, WSResponse}
import play.api.mvc._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import play.api.Play.current

/**
 * Created by shon on 8/5/15.
 */
object ExperimentController extends Controller {
  val impressionKey = "s2ImpressionId"

  def experiment(serviceName: String, experimentKey: String, uuid: String) = Action.async { request =>
    Experiment.find(serviceName, experimentKey) match {
      case None => throw new RuntimeException("not found experiment")
      case Some(experiment) =>
        experiment.findBucket(uuid) match {
          case None => throw new RuntimeException("bucket is not found")
          case Some(bucket) => buildRequest(request, uuid, bucket).map { response =>
            val headers = response.allHeaders map { h =>
              (h._1, h._2.head)
            }
            Result(ResponseHeader(response.status, headers ++ Map(impressionKey -> bucket.impressionId)),
              Enumerator(response.body.getBytes))
          }
        }
    }
  }


  private def buildRequest(request: Request[AnyContent], uuid: String, bucket: Bucket): Future[WSResponse] = {
    var holder = WS.url(bucket.apiPath)
    holder = holder.withHeaders(request.headers.toSimpleMap.toSeq: _*)
    bucket.httpVerb.toLowerCase match {
      case "get" =>
        holder.withQueryString(Bucket.toSimpleMap(request.queryString ++ Map(bucket.uuidKey -> Seq(uuid))).toSeq: _*).get
      case "post" =>
        val body = bucket.requestBody.replace(bucket.uuidPlaceHolder, uuid)
        holder.post(Json.parse(body))
    }
  }


}
