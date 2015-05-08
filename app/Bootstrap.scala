
import play.api.mvc._
import play.api.{ Logger, Application }
import com.daumkakao.s2graph.core.Graph
import com.daumkakao.s2graph.rest.config.Config
import com.daumkakao.s2graph.rest.actors._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import play.api.mvc.WithFilters
import play.filters.gzip.GzipFilter
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

object Global extends WithFilters(LoggingFilter, new GzipFilter()) {

  override def onStart(app: Application) {

    if (Config.IS_WRITE_SERVER && Config.KAFKA_PRODUCER_POOL_SIZE > 0) {
      KafkaAggregatorActor.init()
    }
    val ex = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    Graph(Config.conf)(ex)
  }

  override def onStop(app: Application) {
    if (Config.IS_WRITE_SERVER && Config.KAFKA_PRODUCER_POOL_SIZE > 0) {
      KafkaAggregatorActor.shutdown()
    }

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