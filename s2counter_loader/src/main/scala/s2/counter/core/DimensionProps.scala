package s2.counter.core

import com.kakao.s2graph.core.mysqls.{Bucket, Experiment, Service}
import org.apache.commons.httpclient.HttpStatus
import org.slf4j.LoggerFactory
import play.api.libs.json._
import s2.config.StreamingConfig
import s2.models.Counter
import s2.util.{CollectionCache, CollectionCacheConfig, RetryAsync}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * Created by hsleep(honeysleep@gmail.com) on 2015. 10. 6..
 */
object DimensionProps {
  // using play-ws without play app
  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val client = new play.api.libs.ws.ning.NingWSClient(builder.build)
  private val log = LoggerFactory.getLogger(this.getClass)

  private val retryCnt = 3

  val cacheConfig = CollectionCacheConfig(StreamingConfig.PROFILE_CACHE_MAX_SIZE,
    StreamingConfig.PROFILE_CACHE_TTL_SECONDS,
    negativeCache = true,
    3600 // negative ttl 1 hour
  )
  val cache: CollectionCache[Option[JsValue]] = new CollectionCache[Option[JsValue]](cacheConfig)

  @tailrec
  private[counter] def makeRequestBody(requestBody: String, keyValues: List[(String, String)]): String = {
    keyValues match {
      case head :: tail =>
        makeRequestBody(requestBody.replace(head._1, head._2), tail)
      case Nil => requestBody
    }
  }

  private[counter] def query(bucket: Bucket, item: CounterEtlItem): Future[Option[JsValue]] = {
    val keyValues = (item.dimension.as[JsObject] ++ item.property.as[JsObject] fields)
      .filter { case (key, _) => key.startsWith("[[") && key.endsWith("]]") }
      .map { case (key, jsValue) =>
        val replacement = jsValue match {
          case JsString(s) => s
          case value => value.toString()
        }
        key -> replacement
      }.toList

    val cacheKey = s"${bucket.impressionId}=" + keyValues.flatMap(x => Seq(x._1, x._2)).mkString("_")

    cache.withCacheAsync(cacheKey) {
      val retryFuture = RetryAsync(retryCnt, withSleep = false) {
        val future = bucket.httpVerb.toUpperCase match {
          case "GET" =>
            client.url(bucket.apiPath).get()
          case "POST" =>
            val newBody = makeRequestBody(bucket.requestBody, keyValues)
            client.url(bucket.apiPath).post(Json.parse(newBody))
        }

        future.map { resp =>
          resp.status match {
            case HttpStatus.SC_OK =>
              val json = Json.parse(resp.body)
              for {
                results <- (json \ "results").asOpt[Seq[JsValue]]
                result <- results.headOption
                props <- (result \ "props").asOpt[JsValue]
              } yield {
                props
              }
            case _ =>
              log.error(s"${resp.body}(${resp.status}}) item: $item")
              None
          }
        }
      }

      // if fail to retry
      retryFuture onFailure { case t => log.error(s"${t.getMessage} item: $item") }

      retryFuture
    }
  }

  private[counter] def query(service: Service, experiment: Experiment, item: CounterEtlItem): Future[Option[JsValue]] = {
    val keyValues = (item.dimension.as[JsObject] ++ item.property.as[JsObject] fields)
      .filter { case (key, _) => key.startsWith("[[") && key.endsWith("]]") }.toMap

    val cacheKey = s"${experiment.name}=" + keyValues.flatMap(x => Seq(x._1, x._2)).mkString("_")

    cache.withCacheAsync(cacheKey) {
      val retryFuture = RetryAsync(retryCnt, withSleep = false) {
        val url = s"${StreamingConfig.GRAPH_URL}/graphs/experiment/${service.accessToken}/${experiment.name}/0"
        val future = client.url(url).post(Json.toJson(keyValues))

        future.map { resp =>
          resp.status match {
            case HttpStatus.SC_OK =>
              val json = Json.parse(resp.body)
              for {
                results <- (json \ "results").asOpt[Seq[JsValue]]
                result <- results.headOption
                props <- (result \ "props").asOpt[JsValue]
              } yield {
                props
              }
            case _ =>
              log.error(s"${resp.body}(${resp.status}}) item: $item")
              None
          }
        }
      }

      // if fail to retry
      retryFuture onFailure { case t => log.error(s"${t.getMessage} item: $item") }

      retryFuture
    }
  }

  def mergeDimension(policy: Counter, items: List[CounterEtlItem]): List[CounterEtlItem] = {
    for {
      impId <- policy.bucketImpId
      bucket <- Bucket.findByImpressionId(impId)
      experiment <- Experiment.findById(bucket.experimentId)
      service <- Try { Service.findById(experiment.serviceId) }.toOption
    } yield {
      val futures = {
        for {
          item <- items
        } yield {
          query(service, experiment, item).map {
            case Some(jsValue) =>
              val newDimension = item.dimension.as[JsObject] ++ jsValue.as[JsObject]
              item.copy(dimension = newDimension)
            case None => item
          }
        }
      }
      Await.result(Future.sequence(futures), 10 seconds)
    }
  }.getOrElse(items)

  def getCacheStatsString: String = {
    cache.getStatsString
  }
}
