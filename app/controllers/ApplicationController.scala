package controllers


import config.Config
import play.api.Logger
import play.api.libs.json.JsValue
import play.api.mvc.{Action, AnyContent, BodyParser, Controller, Request, Result}

import scala.concurrent.{ExecutionContext, Future}

object ApplicationController extends Controller with JsonBodyParser {

  var isHealthy = true
  var deployInfo = ""
  val useKeepAlive = Config.USE_KEEP_ALIVE
  var connectionOption = CONNECTION -> "Keep-Alive"
  var keepAliveOption = "Keep-Alive" -> "timeout=5, max=100"

  def updateHealthCheck(isHealthy: Boolean) = Action { request =>
    this.isHealthy = isHealthy
    Ok(this.isHealthy + "\n")
  }

  def healthCheck() = withHeader { request =>
    if (isHealthy) {
      Ok(deployInfo).withHeaders(CONNECTION -> "close")
    } else NotFound.withHeaders(CONNECTION -> "close")
  }

  def toLogMessage[A](request: Request[A], result: Result)(startedAt: Long): String = {
    val duration = System.currentTimeMillis() - startedAt
    val key = s"${request.method} ${request.uri} ${result.header.status.toString}"

    try {
      if (!Config.IS_WRITE_SERVER) {
        s"${request.method} ${request.uri} took ${duration} ms ${result.header.status} ${request.body}"
      } else {
        s"${request.method} ${request.uri} took ${duration} ms ${result.header.status}"
      }
    } finally {
      //      ctx.stop()
    }
  }

  def jsonParser: BodyParser[JsValue] = {
    s2parse.json
  }

  def withHeaderAsync(block: Request[AnyContent] => Future[Result])(implicit ex: ExecutionContext) = {
    Action.async { request =>
      val startedAt = System.currentTimeMillis()
      block(request).map { r =>
        Logger.info(toLogMessage(request, r)(startedAt))
        if (useKeepAlive) if (isHealthy) r.withHeaders(connectionOption, keepAliveOption) else r.withHeaders(CONNECTION -> "close")
        else r.withHeaders(CONNECTION -> "close")
      }
    }
  }

  def withHeaderAsync[A](bodyParser: BodyParser[A])(block: Request[A] => Future[Result])(implicit ex: ExecutionContext) = {
    Action.async(bodyParser) { request =>
      val startedAt = System.currentTimeMillis()
      block(request).map { r =>
        Logger.info(toLogMessage(request, r)(startedAt))
        if (useKeepAlive) if (isHealthy) r.withHeaders(connectionOption, keepAliveOption) else r.withHeaders(CONNECTION -> "close")
        else r.withHeaders(CONNECTION -> "close")
      }
    }
  }

  def withHeader[A](bodyParser: BodyParser[A])(block: Request[A] => Result) = {
    Action(bodyParser) { request: Request[A] =>
      val startedAt = System.currentTimeMillis()
      val r = block(request)
      Logger.info(toLogMessage(request, r)(startedAt))
      if (useKeepAlive) if (isHealthy) r.withHeaders(connectionOption, keepAliveOption) else r.withHeaders(CONNECTION -> "close")
      else r.withHeaders(CONNECTION -> "close")
    }
  }

  def withHeader(block: Request[AnyContent] => Result) = {
    Action { request: Request[AnyContent] =>
      val startedAt = System.currentTimeMillis()
      val r = block(request)
      Logger.info(toLogMessage(request, r)(startedAt))
      if (useKeepAlive) if (isHealthy) r.withHeaders(connectionOption, keepAliveOption) else r.withHeaders(CONNECTION -> "close")
      else r.withHeaders(CONNECTION -> "close")
    }

  }
}
