
import java.util.concurrent.Executors

import com.daumkakao.s2graph.core.{ExceptionHandler, Graph}
import com.daumkakao.s2graph.core.mysqls._
import com.daumkakao.s2graph.rest.actors._
import com.daumkakao.s2graph.rest.config.Config
import controllers.{AdminController, ApplicationController}
import play.api.mvc.{WithFilters, _}
import play.api.{Application, Logger}
import play.filters.gzip.GzipFilter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

object Global extends WithFilters(LoggingFilter, new GzipFilter()) {

  override def onStart(app: Application) {
    ApplicationController.isHealthy = false

    if (Config.IS_WRITE_SERVER && Config.KAFKA_PRODUCER_POOL_SIZE > 0) {
      KafkaAggregatorActor.init()
    }

    val numOfThread = Config.conf.getInt("async.thread.size").getOrElse(Runtime.getRuntime.availableProcessors())
    val threadPool = if (numOfThread == -1) Executors.newCachedThreadPool() else Executors.newFixedThreadPool(numOfThread)
    val ex = ExecutionContext.fromExecutor(threadPool)
    Graph(Config.conf.underlying)(ex)

    Logger.info(s"starts with num of thread: $numOfThread, ${threadPool.getClass.getSimpleName}")

    if (Config.PHASE == "query") {
      val duration = AdminController.warmUpInner()
      Logger.info(s"warmUp took: $duration")
    }

    Service.findAll()
    ServiceColumn.findAll()
    Label.findAll()
    LabelMeta.findAll()
    LabelIndex.findAll()
    ColumnMeta.findAll()

    ApplicationController.isHealthy = true
  }

  override def onStop(app: Application) {
    if (Config.IS_WRITE_SERVER && Config.KAFKA_PRODUCER_POOL_SIZE > 0) {
      KafkaAggregatorActor.shutdown()
    }
    ExceptionHandler.shutdown()

    /**
     * shutdown hbase client for flush buffers.
     */
    for ((zkQuorum, client) <- Graph.clients) {
      client.flush()

      /** to make sure all rpcs just flushed finished. */
      Thread.sleep(client.getFlushInterval * 2)
    }
  }

  override def onError(request: RequestHeader, ex: Throwable): Future[Result] = {
    Logger.error(s"onError => request:${request}", ex)
    Future.successful(Results.InternalServerError)
  }

  override def onHandlerNotFound(request: RequestHeader): Future[Result] = {
    Logger.error(s"onHandlerNotFound => request:${request}")
    Future.successful(Results.NotFound)
  }

  override def onBadRequest(request: RequestHeader, error: String): Future[Result] = {
    Logger.error(s"onBadRequest => request:${request}, error:${error}")
    Future.successful(Results.BadRequest)
  }
}

object LoggingFilter extends EssentialFilter {
  def apply(nextFilter: EssentialAction) = new EssentialAction {
    def apply(requestHeader: RequestHeader) = {
      val start = System.currentTimeMillis

      nextFilter(requestHeader).map { result =>
        val time = System.currentTimeMillis - start
        //        val headers = for (key <- requestHeader.headers.keys; value <- requestHeader.headers.get(key)) yield s"$key:$value" 
        //        .map(kv => s"${kv._1}:${kv._2}").mkString("\t")
        Logger.debug(s"${requestHeader.method} ${requestHeader.uri} took ${time}ms and returned ${result.header.status}")
        result.withHeaders("Request-Time" -> time.toString)
      }
    }
  }
}
