package controllers

import java.util.concurrent.TimeUnit

import play.api.mvc.{Action, Controller, Result, Request, BodyParser, AnyContent}
import play.api.Logger
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.daumkakao.s2graph.rest.actors._
import com.daumkakao.s2graph.rest.config.{Instrumented, Config}
import com.codahale.metrics.Timer

object ApplicationController extends Controller with Instrumented {

  var isHealthy = true
  val useKeepAlive = Config.USE_KEEP_ALIVE
  var connectionOption = CONNECTION -> "Keep-Alive"
  var keepAliveOption = "Keep-Alive" -> "timeout=5, max=100"

  consoleReporter.start(60, TimeUnit.SECONDS)

  def updateHealthCheck(isHealthy: Boolean) = Action { request =>
    this.isHealthy = isHealthy
    Ok(this.isHealthy + "\n")
  }

  def healthCheck() = withHeader { request =>
    if (isHealthy) Ok.withHeaders(CONNECTION -> "close")
    else NotFound.withHeaders(CONNECTION -> "close")
  }


  def toLogMessage[A](request: Request[A], result: Result)(startedAt: Long): String = {
    val duration = System.currentTimeMillis() - startedAt
    val key = s"${request.method} ${request.uri} ${result.header.status.toString}"
    val ctx = getOrElseUpdateMetric[Timer](key)(metricRegistry.timer(key)).time()
    try {
      if (!Config.IS_WRITE_SERVER) {
        s"${request.method} ${request.uri} took ${duration} ms ${result.header.status} ${request.body}"
      } else {
        s"${request.method} ${request.uri} took ${duration} ms ${result.header.status}"
      }
    } finally {
      ctx.stop()
    }
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