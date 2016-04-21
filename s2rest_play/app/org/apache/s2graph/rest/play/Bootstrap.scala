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

package org.apache.s2graph.rest.play

import java.util.concurrent.Executors

import org.apache.s2graph.core.rest.{RequestParser, RestHandler}
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{ExceptionHandler, Graph, Management}
import org.apache.s2graph.rest.play.actors.QueueActor
import org.apache.s2graph.rest.play.config.Config
import org.apache.s2graph.rest.play.controllers.ApplicationController
import play.api.Application
import play.api.mvc.{WithFilters, _}
import play.filters.gzip.GzipFilter

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try

object Global extends WithFilters(new GzipFilter()) {
  var s2graph: Graph = _
  var storageManagement: Management = _
  var s2parser: RequestParser = _
  var s2rest: RestHandler = _
  var wallLogHandler: ExceptionHandler = _

  def startup() = {
    val numOfThread = Runtime.getRuntime.availableProcessors()
    val threadPool = Executors.newFixedThreadPool(numOfThread)
    val ec = ExecutionContext.fromExecutor(threadPool)

    val config = Config.conf.underlying

    // init s2graph with config
    s2graph = new Graph(config)(ec)
    storageManagement = new Management(s2graph)
    s2parser = new RequestParser(s2graph.config) // merged config
    s2rest = new RestHandler(s2graph)(ec)

    logger.info(s"starts with num of thread: $numOfThread, ${threadPool.getClass.getSimpleName}")

    config
  }

  def shutdown() = {
    s2graph.shutdown()
  }

  // Application entry point
  override def onStart(app: Application) {
    ApplicationController.isHealthy = false

    val config = startup()
    wallLogHandler = new ExceptionHandler(config)

    QueueActor.init(s2graph, wallLogHandler)

    val defaultHealthOn = Config.conf.getBoolean("app.health.on").getOrElse(true)
    ApplicationController.deployInfo = Try(Source.fromFile("./release_info").mkString("")).recover { case _ => "release info not found\n" }.get

    ApplicationController.isHealthy = defaultHealthOn
  }

  override def onStop(app: Application) {
    wallLogHandler.shutdown()
    QueueActor.shutdown()

    /*
     * shutdown hbase client for flush buffers.
     */
    shutdown()
  }

  override def onError(request: RequestHeader, ex: Throwable): Future[Result] = {
    logger.error(s"onError => ip:${request.remoteAddress}, request:${request}", ex)
    Future.successful(Results.InternalServerError)
  }

  override def onHandlerNotFound(request: RequestHeader): Future[Result] = {
    logger.error(s"onHandlerNotFound => ip:${request.remoteAddress}, request:${request}")
    Future.successful(Results.NotFound)
  }

  override def onBadRequest(request: RequestHeader, error: String): Future[Result] = {
    logger.error(s"onBadRequest => ip:${request.remoteAddress}, request:$request, error:$error")
    Future.successful(Results.BadRequest(error))
  }
}

