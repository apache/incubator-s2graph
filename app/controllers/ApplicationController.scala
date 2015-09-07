package controllers

import com.daumkakao.s2graph.logger
import config.Config
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.JsValue
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

object ApplicationController extends Controller {

  var isHealthy = true
  var deployInfo = ""
  val useKeepAlive = Config.USE_KEEP_ALIVE
  var connectionKeepAlive = CONNECTION -> "keep-alive"
  var connectionClose = CONNECTION -> "close"
  val applicationJsonHeader = "application/json"

  def jsonParser: BodyParser[JsValue] = controllers.s2parse.json

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
      ).as(applicationJsonHeader)
    }

  def responseWithConnectionHeader(r: Result): Result = {
    if (useKeepAlive && isHealthy) r.withHeaders(connectionKeepAlive)
    else r.withHeaders(connectionClose)
  }

  def toLogMessage[A](request: Request[A], result: Result)(startedAt: Long): String = {
    val duration = System.currentTimeMillis() - startedAt
    val resultSize = result.header.headers.getOrElse("result_size", "0")

    try {
      s"${request.method} ${request.uri} took ${duration} ms ${result.header.status} ${resultSize} ${request.body}"
    } finally {
      /* pass */
    }
  }

  def withHeaderAsync[A](bodyParser: BodyParser[A])(block: Request[A] => Future[Result])(implicit ex: ExecutionContext) =
    Action.async(bodyParser) { request =>
      val startedAt = System.currentTimeMillis()
      block(request).map { r =>
        logger.info(toLogMessage(request, r)(startedAt))
        responseWithConnectionHeader(r)
      }
    }

  def withHeader[A](bodyParser: BodyParser[A])(block: Request[A] => Result) =
    Action(bodyParser) { request: Request[A] =>
      val startedAt = System.currentTimeMillis()
      val r = block(request)
      logger.info(toLogMessage(request, r)(startedAt))
      responseWithConnectionHeader(r)
    }
}
