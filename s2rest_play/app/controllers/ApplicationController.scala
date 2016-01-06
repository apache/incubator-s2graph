package controllers

import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core.PostProcess
import com.kakao.s2graph.core.utils.logger
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.{JsString, JsValue, Json}
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

object ApplicationController extends Controller {

  var isHealthy = true
  var deployInfo = ""
  val applicationJsonHeader = "application/json"

  val jsonParser: BodyParser[JsValue] = controllers.s2parse.json

  private def badQueryExceptionResults(ex: Exception) =
    Future.successful(BadRequest(PostProcess.badRequestResults(ex)).as(applicationJsonHeader))

  private def errorResults =
    Future.successful(Ok(PostProcess.emptyResults).as(applicationJsonHeader))

  def requestFallback(body: JsValue): PartialFunction[Throwable, Future[Result]] = {
    case e: BadQueryException =>
      logger.error(s"{$body}, ${e.getMessage}", e)
      badQueryExceptionResults(e)
    case e: Exception =>
      logger.error(s"${body}, ${e.getMessage}", e)
      errorResults
  }

  def updateHealthCheck(isHealthy: Boolean) = Action { request =>
    this.isHealthy = isHealthy
    Ok(this.isHealthy + "\n")
  }

  def healthCheck() = withHeader(parse.anyContent) { request =>
    if (isHealthy) Ok(deployInfo)
    else NotFound
  }

  def jsonResponse(json: JsValue, headers: (String, String)*) =
    if (ApplicationController.isHealthy) {
      Ok(json).as(applicationJsonHeader).withHeaders(headers: _*)
    } else {
      Result(
        header = ResponseHeader(OK),
        body = Enumerator(json.toString.getBytes()),
        connection = HttpConnection.Close
      ).as(applicationJsonHeader).withHeaders((CONNECTION -> "close") +: headers: _*)
    }

  def toLogMessage[A](request: Request[A], result: Result)(startedAt: Long): String = {
    val duration = System.currentTimeMillis() - startedAt
    val isQueryRequest = result.header.headers.contains("result_size")
    val resultSize = result.header.headers.getOrElse("result_size", "-1")

    try {
      val body = request.body match {
        case AnyContentAsJson(jsValue) => jsValue match {
          case JsString(str) => str
          case _ => jsValue.toString
        }
        case _ => request.body.toString
      }

      val str =
        if (isQueryRequest)
          s"${request.method} ${request.uri} took ${duration} ms ${result.header.status} ${resultSize} ${body}"
        else
          s"${request.method} ${request.uri} took ${duration} ms ${result.header.status} ${resultSize} ${body}"

      str
    } finally {
      /* pass */
    }
  }

  def withHeaderAsync[A](bodyParser: BodyParser[A])(block: Request[A] => Future[Result])(implicit ex: ExecutionContext) =
    Action.async(bodyParser) { request =>
      val startedAt = System.currentTimeMillis()
      block(request).map { r =>
        logger.info(toLogMessage(request, r)(startedAt))
        r
      }
    }

  def withHeader[A](bodyParser: BodyParser[A])(block: Request[A] => Result) =
    Action(bodyParser) { request: Request[A] =>
      val startedAt = System.currentTimeMillis()
      val r = block(request)
      logger.info(toLogMessage(request, r)(startedAt))
      r
    }
}
