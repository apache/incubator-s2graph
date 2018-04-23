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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.language.postfixOps

object Server extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem("s2graphql-server")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  import scala.concurrent.duration._

  val route: Flow[HttpRequest, HttpResponse, Any] = (post & path("graphql")) {
    entity(as[spray.json.JsValue])(GraphQLServer.endpoint)
  } ~ {
    getFromResource("assets/graphiql.html")
  }

  val port = sys.props.get("http.port").fold(8000)(_.toInt)

  logger.info(s"Starting GraphQL server... ${port}")
  Http().bindAndHandle(route, "0.0.0.0", port)

  def shutdown(): Unit = {
    logger.info("Terminating...")

    system.terminate()
    Await.result(system.whenTerminated, 30 seconds)

    logger.info("Terminated.")
  }
}
