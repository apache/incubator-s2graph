package org.apache.s2graph.http

import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.rest.RestHandler.CanLookup
import org.slf4j.LoggerFactory
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model._
import org.apache.s2graph.core.GraphExceptions.{BadQueryException, JsonParseException}
import org.apache.s2graph.core.rest.RestHandler
import play.api.libs.json._

object S2GraphTraversalRoute {

  import scala.collection._

  implicit val akkHttpHeaderLookup = new CanLookup[immutable.Seq[HttpHeader]] {
    override def lookup(m: immutable.Seq[HttpHeader], key: String): Option[String] = m.find(_.name() == key).map(_.value())
  }
}

trait S2GraphTraversalRoute extends PlayJsonSupport {

  import S2GraphTraversalRoute._

  val s2graph: S2Graph
  val logger = LoggerFactory.getLogger(this.getClass)

  implicit lazy val ec = s2graph.ec
  lazy val restHandler = new RestHandler(s2graph)

  // The `/graphs/*` APIs are implemented to be branched from the existing restHandler.doPost.
  // Implement it first by delegating that function.
  lazy val delegated: Route = {
    entity(as[String]) { body =>
      logger.info(body)

      extractRequest { request =>
        val result = restHandler.doPost(request.uri.toRelative.toString(), body, request.headers)
        val responseHeaders = result.headers.toList.map { case (k, v) => RawHeader(k, v) }

        val f = result.body.map(StatusCodes.OK -> _).recover {
          case BadQueryException(msg, _) => StatusCodes.BadRequest -> Json.obj("error" -> msg)
          case JsonParseException(msg) => StatusCodes.BadRequest -> Json.obj("error" -> msg)
          case e: Exception => StatusCodes.InternalServerError -> Json.obj("error" -> e.toString)
        }

        respondWithHeaders(responseHeaders)(complete(f))
      }
    }
  }

  // expose routes
  lazy val traversalRoute: Route =
    post {
      concat(
        delegated // getEdges, experiments, etc.
      )
    }
}

