package org.apache.s2graph.http

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.fasterxml.jackson.core.JsonParseException
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}

trait S2GraphMutateRoute extends PlayJsonSupport {

  val s2graph: S2Graph
  val logger = LoggerFactory.getLogger(this.getClass)

  lazy val parser = new RequestParser(s2graph)

  lazy val exceptionHandler = ExceptionHandler {
    case ex: JsonParseException => complete(StatusCodes.BadRequest -> ex.getMessage)
    case ex: java.lang.IllegalArgumentException => complete(StatusCodes.BadRequest -> ex.getMessage)
  }

  lazy val mutateVertex = path("vertex" / Segments) { params =>
    implicit val ec = s2graph.ec

    val (operation, serviceNameOpt, columnNameOpt) = params match {
      case operation :: serviceName :: columnName :: Nil => (operation, Option(serviceName), Option(columnName))
      case operation :: Nil => (operation, None, None)
      case _ => throw new RuntimeException("invalid params")
    }

    entity(as[JsValue]) { payload =>
      val future = vertexMutate(payload, operation, serviceNameOpt, columnNameOpt).map(Json.toJson(_))

      complete(future)
    }
  }

  lazy val mutateEdge = path("edge" / Segment) { operation =>
    implicit val ec = s2graph.ec

    entity(as[JsValue]) { payload =>
      val future = edgeMutate(payload, operation, withWait = true).map(Json.toJson(_))

      complete(future)
    }
  }

  def vertexMutate(jsValue: JsValue,
                   operation: String,
                   serviceNameOpt: Option[String] = None,
                   columnNameOpt: Option[String] = None,
                   withWait: Boolean = true)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = {
    val vertices = parser.toVertices(jsValue, operation, serviceNameOpt, columnNameOpt)

    val verticesToStore = vertices.filterNot(_.isAsync)

    s2graph.mutateVertices(verticesToStore, withWait).map(_.map(_.isSuccess))
  }

  def edgeMutate(elementsWithTsv: Seq[(GraphElement, String)], withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = {
    val elementWithIdxs = elementsWithTsv.zipWithIndex
    val (elementSync, elementAsync) = elementWithIdxs.partition { case ((element, tsv), idx) => !element.isAsync }

    val retToSkip = elementAsync.map(_._2 -> MutateResponse.Success)
    val (elementsToStore, _) = elementSync.map(_._1).unzip
    val elementsIdxToStore = elementSync.map(_._2)

    s2graph.mutateElements(elementsToStore, withWait).map { mutateResponses =>
      elementsIdxToStore.zip(mutateResponses) ++ retToSkip
    }.map(_.sortBy(_._1).map(_._2.isSuccess))
  }

  def edgeMutate(jsValue: JsValue, operation: String, withWait: Boolean)(implicit ec: ExecutionContext): Future[Seq[Boolean]] = {
    val edgesWithTsv = parser.parseJsonFormat(jsValue, operation)
    edgeMutate(edgesWithTsv, withWait)
  }

  // expose routes
  lazy val mutateRoute: Route =
    post {
      concat(
        handleExceptions(exceptionHandler) {
          mutateVertex
        },
        handleExceptions(exceptionHandler) {
          mutateEdge
        }
      )
    }

}

