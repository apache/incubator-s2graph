
import java.util.concurrent.Executors

import actors.QueueActor
import com.kakao.s2graph.core.utils.logger
import com.kakao.s2graph.core.{ExceptionHandler, Graph}
import config.Config
import controllers.{AdminController, ApplicationController}
import play.api.Application
import play.api.mvc.{WithFilters, _}
import play.filters.gzip.GzipFilter

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try

object Global extends WithFilters(new GzipFilter()) {

  override def onStart(app: Application) {
    QueueActor.init()

    ApplicationController.isHealthy = false

    if (Config.IS_WRITE_SERVER && Config.KAFKA_PRODUCER_POOL_SIZE > 0) {
      ExceptionHandler.init()
    }

    val numOfThread = Config.conf.getInt("async.thread.size").getOrElse(Runtime.getRuntime.availableProcessors())
    val threadPool = if (numOfThread == -1) Executors.newCachedThreadPool() else Executors.newFixedThreadPool(numOfThread)
    val ex = ExecutionContext.fromExecutor(threadPool)
    Graph(Config.conf.underlying)(ex)

    logger.info(s"starts with num of thread: $numOfThread, ${threadPool.getClass.getSimpleName}")

    val defaultHealthOn = Config.conf.getBoolean("app.health.on").getOrElse(true)
    ApplicationController.deployInfo = Try(Source.fromFile("./release_info").mkString("")).recover { case _ => "release info not found\n" }.get

    AdminController.loadCacheInner()
    ApplicationController.isHealthy = defaultHealthOn
  }

  override def onStop(app: Application) {
    QueueActor.shutdown()

    if (Config.IS_WRITE_SERVER && Config.KAFKA_PRODUCER_POOL_SIZE > 0) {
      ExceptionHandler.shutdown()
    }

    /**
     * shutdown hbase client for flush buffers.
     */
    Graph.flush
  }

  override def onError(request: RequestHeader, ex: Throwable): Future[Result] = {
    logger.error(s"onError => ip:${request.remoteAddress}, request:${request}", ex)
    Future.successful(Results.InternalServerError)
  }

  override def onHandlerNotFound(request: RequestHeader): Future[Result] = {
    logger.error(s"onHandlerNotFound => ip:${request.remoteAddress}, request:${request}")
    Future.successful(Results.NotFound)
  }

  override def onBadRequest(request: RequestHeader, error: String): Future[Result] = {
    logger.error(s"onBadRequest => ip:${request.remoteAddress}, request:$request, error:$error")
    Future.successful(Results.BadRequest(error))
  }
}
