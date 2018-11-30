package org.apache.s2graph.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.graphql.GraphQLServer
import org.slf4j.LoggerFactory
import sangria.ast.Document
import sangria.execution.{ErrorWithResolver, QueryAnalysisError}
import sangria.parser.QueryParser
import spray.json.{JsNull, JsObject, JsString, JsValue}

import scala.util.{Failure, Left, Right, Success, Try}

object S2GraphQLRoute {
  def parseJson(jsStr: String): Either[Throwable, JsValue] = {
    import spray.json._
    val parsed = Try(jsStr.parseJson)

    parsed match {
      case Success(js) => Right(js)
      case Failure(e) => Left(e)
    }
  }
}

trait S2GraphQLRoute extends SprayJsonSupport with SangriaGraphQLSupport {

  import S2GraphQLRoute._
  import spray.json.DefaultJsonProtocol._
  import sangria.marshalling.sprayJson._

  val s2graph: S2Graph
  val logger = LoggerFactory.getLogger(this.getClass)

  lazy val graphQLServer = new GraphQLServer(s2graph)

  private val exceptionHandler = ExceptionHandler {
    case error: QueryAnalysisError =>
      logger.error("Error on execute", error)
      complete(StatusCodes.BadRequest -> error.resolveError)
    case error: ErrorWithResolver =>
      logger.error("Error on execute", error)
      complete(StatusCodes.InternalServerError -> error.resolveError)
  }

  lazy val updateEdgeFetcher = path("updateEdgeFetcher") {
    entity(as[spray.json.JsValue]) { body =>
      graphQLServer.updateEdgeFetcher(body)(s2graph.ec) match {
        case Success(_) => complete(StatusCodes.OK -> JsString("Update fetcher finished"))
        case Failure(e) =>
          logger.error("Error on execute", e)
          complete(StatusCodes.InternalServerError -> spray.json.JsObject("message" -> JsString(e.toString)))
      }
    }
  }

  lazy val graphql = parameters('operationName.?, 'variables.?) { (operationNameParam, variablesParam) =>
    implicit val ec = s2graph.ec

    entity(as[Document]) { document ⇒
      variablesParam.map(parseJson) match {
        case None ⇒ complete(graphQLServer.executeQuery(document, operationNameParam, JsObject()))
        case Some(Right(js)) ⇒ complete(graphQLServer.executeQuery(document, operationNameParam, js.asJsObject))
        case Some(Left(e)) ⇒
          logger.error("Error on execute", e)
          complete(StatusCodes.BadRequest -> GraphQLServer.formatError(e))
      }
    } ~ entity(as[spray.json.JsValue]) { body ⇒
      val fields = body.asJsObject.fields

      val query = fields.get("query").map(js => js.convertTo[String])
      val operationName = fields.get("operationName").filterNot(_ == JsNull).map(_.convertTo[String])
      val variables = fields.get("variables").filterNot(_ == JsNull)

      query.map(QueryParser.parse(_)) match {
        case None ⇒ complete(StatusCodes.BadRequest -> GraphQLServer.formatError("No query to execute"))
        case Some(Failure(error)) ⇒
          logger.error("Error on execute", error)
          complete(StatusCodes.BadRequest -> GraphQLServer.formatError(error))
        case Some(Success(document)) => variables match {
          case Some(js) ⇒ complete(graphQLServer.executeQuery(document, operationName, js.asJsObject))
          case None ⇒ complete(graphQLServer.executeQuery(document, operationName, JsObject()))
        }
      }
    }
  }

  // expose routes
  lazy val graphqlRoute: Route =
    get {
      getFromResource("assets/graphiql.html")
    } ~
      post {
        handleExceptions(exceptionHandler) {
          concat(
            updateEdgeFetcher,
            graphql
          )
        }
      }
}

