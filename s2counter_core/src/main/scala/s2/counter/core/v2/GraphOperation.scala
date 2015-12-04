package s2.counter.core.v2

import com.typesafe.config.Config
import org.apache.http.HttpStatus
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, JsValue, Json}
import s2.config.S2CounterConfig

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 10..
  */
class GraphOperation(config: Config) {
  // using play-ws without play app
  private val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  private val wsClient = new play.api.libs.ws.ning.NingWSClient(builder.build)
  private val s2config = new S2CounterConfig(config)
  val s2graphUrl = s2config.GRAPH_URL
  private[counter] val log = LoggerFactory.getLogger(this.getClass)

  import scala.concurrent.ExecutionContext.Implicits.global

  def createLabel(json: JsValue): Boolean = {
    // fix counter label's schemaVersion
    val future = wsClient.url(s"$s2graphUrl/graphs/createLabel").post(json).map { resp =>
      resp.status match {
        case HttpStatus.SC_OK =>
          true
        case _ =>
          throw new RuntimeException(s"failed createLabel. errCode: ${resp.status} body: ${resp.body} query: $json")
      }
    }

    Await.result(future, 10 second)
  }

  def deleteLabel(label: String): Boolean = {
    val future = wsClient.url(s"$s2graphUrl/graphs/deleteLabel/$label").put("").map { resp =>
      resp.status match {
        case HttpStatus.SC_OK =>
          true
        case _ =>
          throw new RuntimeException(s"failed deleteLabel. errCode: ${resp.status} body: ${resp.body}")
      }
    }

    Await.result(future, 10 second)
  }
}
