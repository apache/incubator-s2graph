/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.graphql

import java.nio.charset.Charset

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, StandardRoute}
import akka.stream.ActorMaterializer
import org.slf4j.LoggerFactory
import sangria.parser.QueryParser
import spray.json._

import scala.concurrent.Await
import scala.language.postfixOps
import scala.util._
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller, ToResponseMarshallable}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import sangria.ast.Document
import sangria.renderer.{QueryRenderer, QueryRendererConfig}

import scala.collection.immutable.Seq

object Server extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem("s2graphql-server")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher
  import scala.concurrent.duration._

  import spray.json.DefaultJsonProtocol._

  val route: Route =
    get {
      getFromResource("assets/graphiql.html")
    } ~ (post & path("updateEdgeFetcher")) {
      entity(as[JsValue]) { body =>
        GraphQLServer.updateEdgeFetcher(body) match {
          case Success(_) => complete(StatusCodes.OK -> JsString("Update fetcher finished"))
          case Failure(e) =>
            logger.error("Error on execute", e)
            complete(StatusCodes.InternalServerError -> spray.json.JsObject("message" -> JsString(e.toString)))
        }
      }
    } ~ (post & path("graphql")) {
      parameters('operationName.?, 'variables.?) { (operationNameParam, variablesParam) =>
        entity(as[Document]) { document ⇒
          variablesParam.map(parseJson) match {
            case None ⇒ complete(GraphQLServer.executeGraphQLQuery(document, operationNameParam, JsObject()))
            case Some(Right(js)) ⇒ complete(GraphQLServer.executeGraphQLQuery(document, operationNameParam, js.asJsObject))
            case Some(Left(e)) ⇒
              logger.error("Error on execute", e)
              complete(StatusCodes.BadRequest -> GraphQLServer.formatError(e))
          }
        } ~ entity(as[JsValue]) { body ⇒
          val fields = body.asJsObject.fields

          val query = fields.get("query").map(js => js.convertTo[String])
          val operationName = fields.get("operationName").filterNot(_ == null).map(_.convertTo[String])
          val variables = fields.get("variables").filterNot(_ == null)

          query.map(QueryParser.parse(_)) match {
            case None ⇒ complete(StatusCodes.BadRequest -> GraphQLServer.formatError("No query to execute"))
            case Some(Failure(error)) ⇒
              logger.error("Error on execute", error)
              complete(StatusCodes.BadRequest -> GraphQLServer.formatError(error))
            case Some(Success(document)) => variables match {
              case Some(js) ⇒ complete(GraphQLServer.executeGraphQLQuery(document, operationName, js.asJsObject))
              case None ⇒ complete(GraphQLServer.executeGraphQLQuery(document, operationName, JsObject()))
            }
          }
        }
      }
    }

  val port = sys.props.get("http.port").fold(8000)(_.toInt)

  logger.info(s"Starting GraphQL server... $port")

  Http().bindAndHandle(route, "0.0.0.0", port)

  def shutdown(): Unit = {
    logger.info("Terminating...")

    system.terminate()
    Await.result(system.whenTerminated, 30 seconds)

    logger.info("Terminated.")
  }

  // Unmarshaller

  def unmarshallerContentTypes: Seq[ContentTypeRange] = mediaTypes.map(ContentTypeRange.apply)

  def mediaTypes: Seq[MediaType.WithFixedCharset] =
    Seq(MediaType.applicationWithFixedCharset("graphql", HttpCharsets.`UTF-8`, "graphql"))

  implicit def documentMarshaller(implicit config: QueryRendererConfig = QueryRenderer.Compact): ToEntityMarshaller[Document] = {
    Marshaller.oneOf(mediaTypes: _*) {
      mediaType ⇒
        Marshaller.withFixedContentType(ContentType(mediaType)) {
          json ⇒ HttpEntity(mediaType, QueryRenderer.render(json, config))
        }
    }
  }

  implicit val documentUnmarshaller: FromEntityUnmarshaller[Document] = {
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(unmarshallerContentTypes: _*)
      .map {
        case ByteString.empty ⇒ throw Unmarshaller.NoContentException
        case data ⇒
          import sangria.parser.DeliveryScheme.Throw
          QueryParser.parse(data.decodeString(Charset.forName("UTF-8")))
      }
  }

  def parseJson(jsStr: String): Either[Throwable, JsValue] = {
    val parsed = Try(jsStr.parseJson)
    parsed match {
      case Success(js) => Right(js)
      case Failure(e) => Left(e)
    }
  }

}
