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

package org.apache.s2graph.rest.play.controllers

import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{ExceptionHandler, Graph, GraphExceptions}
import org.apache.s2graph.rest.play.actors.QueueActor
import org.apache.s2graph.rest.play.config.Config
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Controller, Result}

import scala.concurrent.Future

object VertexController extends Controller {
  private val s2: Graph = org.apache.s2graph.rest.play.Global.s2graph
  private val requestParser: RequestParser = org.apache.s2graph.rest.play.Global.s2parser
  private val walLogHandler: ExceptionHandler = org.apache.s2graph.rest.play.Global.wallLogHandler

  import ApplicationController._
  import ExceptionHandler._
  import play.api.libs.concurrent.Execution.Implicits._

  def tryMutates(jsValue: JsValue, operation: String, serviceNameOpt: Option[String] = None, columnNameOpt: Option[String] = None, withWait: Boolean = false): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)
    else {
      try {
        val vertices = requestParser.toVertices(jsValue, operation, serviceNameOpt, columnNameOpt)

        for (vertex <- vertices) {
          val kafkaTopic = toKafkaTopic(vertex.isAsync)
          walLogHandler.enqueue(toKafkaMessage(kafkaTopic, vertex, None))
        }

        //FIXME:
        val verticesToStore = vertices.filterNot(v => v.isAsync)

        if (withWait) {
          val rets = s2.mutateVertices(verticesToStore, withWait = true)
          rets.map(Json.toJson(_)).map(jsonResponse(_))
        } else {
          val rets = verticesToStore.map { vertex => QueueActor.router ! vertex; true }
          Future.successful(jsonResponse(Json.toJson(rets)))
        }
      } catch {
        case e: GraphExceptions.JsonParseException => Future.successful(BadRequest(s"e"))
        case e: Exception =>
          logger.error(s"[Failed] tryMutates", e)
          Future.successful(InternalServerError(s"${e.getStackTrace}"))
      }
    }
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert")
  }

  def insertsWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert", withWait = true)
  }

  def insertsSimple(serviceName: String, columnName: String) = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert", Some(serviceName), Some(columnName))
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete")
  }

  def deletesWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete", withWait = true)
  }

  def deletesSimple(serviceName: String, columnName: String) = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete", Some(serviceName), Some(columnName))
  }

  def deletesAll() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "deleteAll")
  }

  def deletesAllSimple(serviceName: String, columnName: String) = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "deleteAll", Some(serviceName), Some(columnName))
  }

}
