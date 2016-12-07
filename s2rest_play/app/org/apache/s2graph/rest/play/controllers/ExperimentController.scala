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
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

object ExperimentController extends Controller {
  private val rest: RestHandler = org.apache.s2graph.rest.play.Global.s2rest

  import ApplicationController._

  def experiments(): Action[String] = experiment("", "", "")
  def experiment(accessToken: String, experimentName: String, uuid: String): Action[String] =
    withHeaderAsync(jsonText) { request =>
      val body = request.body
      val res = rest.doPost(request.uri, body, request.headers)
      res.body.map {
        case js =>
          val headers = res.headers :+ ("result_size" -> rest
              .calcSize(js)
              .toString)
          jsonResponse(js, headers: _*)
      } recoverWith ApplicationController.requestFallback(body)
    }
}
