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

import akka.util.ByteString
import org.apache.s2graph.core.GraphExceptions.BadQueryException
import org.apache.s2graph.core.PostProcess
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.rest.play.config.Config
import play.api.http.HttpEntity
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.{JsString, JsValue}
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

object ApplicationController extends Controller {

  var isHealthy = true
  var isWriteFallbackHealthy= true
  var deployInfo = ""
  val applicationJsonHeader = "application/json"

  val jsonParser: BodyParser[JsValue] = s2parse.json

  val jsonText: BodyParser[String] = s2parse.jsonText

  private def badQueryExceptionResults(ex: Exception) =
    Future.successful(BadRequest(PostProcess.badRequestResults(ex)).as(applicationJsonHeader))

  private def errorResults =
    Future.successful(Ok(PostProcess.emptyResults).as(applicationJsonHeader))

  def requestFallback(body: String): PartialFunction[Throwable, Future[Result]] = {
    case e: BadQueryException =>
      logger.error(s"{$body}, ${e.getMessage}", e)
      badQueryExceptionResults(e)
    case e: Exception =>
      logger.error(s"${body}, ${e.getMessage}", e)
      errorResults
  }

  def updateHealthCheck(isHealthy: Boolean) = Action { request =>
    this.isHealthy = isHealthy
    Ok(this.isHealthy + "\n")
  }

  def healthCheck() = withHeader(parse.anyContent) { request =>
    if (isHealthy) Ok(deployInfo)
    else NotFound
  }

  def toKafkaTopic(isAsync: Boolean) = {
    if (!isWriteFallbackHealthy) Config.KAFKA_FAIL_TOPIC
    else {
      if (isAsync) Config.KAFKA_LOG_TOPIC_ASYNC else Config.KAFKA_LOG_TOPIC
    }
  }

  def jsonResponse(json: JsValue, headers: (String, String)*) =
    if (ApplicationController.isHealthy) {
      Ok(json).as(applicationJsonHeader).withHeaders(headers: _*)
    } else {
      Result(
        header = ResponseHeader(OK),
        body = HttpEntity.Strict(ByteString(json.toString.getBytes()), Some(applicationJsonHeader))
      ).as(applicationJsonHeader).withHeaders((CONNECTION -> "close") +: headers: _*)
    }

  def toLogMessage[A](request: Request[A], result: Result)(startedAt: Long): String = {
    val duration = System.currentTimeMillis() - startedAt
    val isQueryRequest = result.header.headers.contains("result_size")
    val resultSize = result.header.headers.getOrElse("result_size", "-1")

    try {
      val body = request.body match {
        case AnyContentAsJson(jsValue) => jsValue match {
          case JsString(str) => str
          case _ => jsValue.toString
        }
        case AnyContentAsEmpty => ""
        case _ => request.body.toString
      }

      val str =
        if (isQueryRequest)
          s"${request.method} ${request.uri} took ${duration} ms ${result.header.status} ${resultSize} ${body}"
        else
          s"${request.method} ${request.uri} took ${duration} ms ${result.header.status} ${resultSize} ${body}"

      str
    } finally {
      /* pass */
    }
  }

  def withHeaderAsync[A](bodyParser: BodyParser[A])(block: Request[A] => Future[Result])(implicit ex: ExecutionContext) =
    Action.async(bodyParser) { request =>
      val startedAt = System.currentTimeMillis()
      block(request).map { r =>
        logger.info(toLogMessage(request, r)(startedAt))
        r
      }
    }

  def withHeader[A](bodyParser: BodyParser[A])(block: Request[A] => Result) =
    Action(bodyParser) { request: Request[A] =>
      val startedAt = System.currentTimeMillis()
      val r = block(request)
      logger.info(toLogMessage(request, r)(startedAt))
      r
    }
}
