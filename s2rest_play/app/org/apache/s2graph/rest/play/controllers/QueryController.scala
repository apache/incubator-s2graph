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

import org.apache.s2graph.core.rest.RestHandler
import play.api.libs.json.Json
import play.api.mvc._

import scala.concurrent.Future
import scala.language.postfixOps

object QueryController extends Controller {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  private val rest: RestHandler = org.apache.s2graph.rest.play.Global.s2rest

  def delegate(request: Request[String]): Future[Result] =
    rest.doPost(request.uri, request.body, request.headers).body.map { js =>
      jsonResponse(js, "result_size" -> rest.calcSize(js).toString)
    } recoverWith ApplicationController.requestFallback(request.body)

  def getEdges(): Action[String] = withHeaderAsync(jsonText)(delegate)

  def getEdgesWithGrouping(): Action[String] =
    withHeaderAsync(jsonText)(delegate)

  def getEdgesExcluded(): Action[String] = withHeaderAsync(jsonText)(delegate)

  def getEdgesExcludedWithGrouping(): Action[String] =
    withHeaderAsync(jsonText)(delegate)

  def checkEdges(): Action[String] = withHeaderAsync(jsonText)(delegate)

  def getEdgesGrouped(): Action[String] = withHeaderAsync(jsonText)(delegate)

  def getEdgesGroupedExcluded(): Action[String] =
    withHeaderAsync(jsonText)(delegate)

  def getEdgesGroupedExcludedFormatted(): Action[String] =
    withHeaderAsync(jsonText)(delegate)

  def getEdge(srcId: String, tgtId: String, labelName: String, direction: String): Action[String] =
    withHeaderAsync(jsonText) { request =>
      val params = Json.arr(
        Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId)
      )
      rest.checkEdges(params).body.map { js =>
        jsonResponse(js, "result_size" -> rest.calcSize(js).toString)
      } recoverWith ApplicationController.requestFallback(request.body)
    }

  def getVertices(): Action[String] = withHeaderAsync(jsonText)(delegate)
}
