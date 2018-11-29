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

package org.apache.s2graph.http

import scala.language.postfixOps
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.S2Graph
import org.slf4j.LoggerFactory

object Server extends App
  with S2GraphTraversalRoute
  with S2GraphAdminRoute
  with S2GraphMutateRoute {

  implicit val system: ActorSystem = ActorSystem("S2GraphHttpServer")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  val config = ConfigFactory.load()

  override val s2graph = new S2Graph(config)
  override val logger = LoggerFactory.getLogger(this.getClass)

  val port = sys.props.get("http.port").fold(8000)(_.toInt)
  val serverStatus = s""" { "port": ${port}, "started_at": ${System.currentTimeMillis()} }"""

  val health = HttpResponse(status = StatusCodes.OK, entity = HttpEntity(ContentTypes.`application/json`, serverStatus))

  // Allows you to determine routes to expose according to external settings.
  lazy val routes: Route = concat(
    pathPrefix("graphs")(traversalRoute),
    pathPrefix("mutate")(mutateRoute),
    pathPrefix("admin")(adminRoute),
    get(complete(health))
  )

  val binding: Future[Http.ServerBinding] = Http().bindAndHandle(routes, "localhost", port)
  binding.onComplete {
    case Success(bound) => logger.info(s"Server online at http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/")
    case Failure(e) =>
      logger.error(s"Server could not start!", e)
      system.terminate()
  }

  Await.result(system.whenTerminated, Duration.Inf)
}
