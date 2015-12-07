package com.kakao.s2graph.rest.finagle

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.utils.logger
import com.twitter
import com.twitter.finagle
import com.twitter.finagle.http.HttpMuxer
import com.twitter.finagle.{Service, _}
import com.twitter.util._
import com.typesafe.config.ConfigFactory
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext
import scala.util.Success

object FinagleServer extends App {
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  implicit val ec = ExecutionContext.fromExecutor(threadPool)

  val config = ConfigFactory.load()

  // init s2graph with config
  val s2graph = new Graph(config)(ec)
  val s2parser = new RequestParser(s2graph)

  // app status code
  var isHealthy = true

  val service = new Service[http.Request, http.Response] {
    def apply(req: http.Request): Future[http.Response] = {
      val promise = new twitter.util.Promise[http.Response]
      val startedAt = System.currentTimeMillis()

      req.method match {
        case http.Method.Post =>
          req.path match {
            case path if path.startsWith("/graphs/getEdges") =>
              val payload = req.contentString
              val bodyAsJson = Json.parse(payload)
              val query = s2parser.toQuery(bodyAsJson)
              val fetch = s2graph.getEdges(query)

              fetch.onComplete {
                case Success(queryRequestWithResutLs) =>
                  val jsValue = PostProcess.toSimpleVertexArrJson(queryRequestWithResutLs, Nil)

                  val httpRes = {
                    val response = http.Response(finagle.http.Version.Http11, finagle.http.Status.Ok)
                    response.setContentTypeJson()
                    response.setContentString(jsValue.toString)
                    response
                  }

                  val duration = System.currentTimeMillis() - startedAt
                  val resultSize = -1 // TODO
                val str = s"${req.method} ${req.uri} took ${duration} ms ${200} ${resultSize} ${payload}"

                  logger.info(str)

                  promise.become(Future.value(httpRes))
              }
          }

        case finagle.http.Method.Get =>
          req.path match {
            case path if path.startsWith("/health_check") =>
              val httpRes = {
                val response = finagle.http.Response(finagle.http.Version.Http11, finagle.http.Status.Ok)
                response.setContentType("text/plain")
                response.setContentString(isHealthy.toString)
                response
              }
              promise.become(Future.value(httpRes))
          }

        case finagle.http.Method.Put =>
          req.path match {
            case path if path.startsWith("/health_check") =>
              val op = path.split("/").last.toBoolean
              isHealthy = op

              val httpRes = {
                val response = finagle.http.Response(finagle.http.Version.Http11, finagle.http.Status.Ok)
                response.setContentType("text/plain")
                response.setContentString(isHealthy.toString)
                response
              }
              promise.become(Future.value(httpRes))
          }
      }

      promise
    }
  }

  val port = try config.getInt("http.port") catch {
    case e: Exception => 9000
  }
  import com.twitter.finagle.builder.{Server, ServerBuilder}
  import com.twitter.finagle.http._
  import com.twitter.finagle.{Service, SimpleFilter}
  import com.twitter.io.Charsets.Utf8
  import com.twitter.util.Future
  import java.net.InetSocketAddress

  val server: Server = ServerBuilder()
    .codec(Http())
    .backlog(2048)
    .bindTo(new InetSocketAddress(port))
    .name("s2graph-rest")
    .build(service)

  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run(): Unit = {
      s2graph.shutdown()
      Await.ready(server.close())
    }
  })

  Await.ready(server)
}

