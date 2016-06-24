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

import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.Label
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.rest.play.actors.QueueActor
import org.apache.s2graph.rest.play.config.Config
import play.api.libs.json._
import play.api.mvc.{Controller, Result}

import scala.collection.Seq
import scala.concurrent.Future
import scala.util.Random

object EdgeController extends Controller {

  import ApplicationController._
  import play.api.libs.concurrent.Execution.Implicits._

  private val s2: Graph = org.apache.s2graph.rest.play.Global.s2graph
  private val requestParser: RequestParser = org.apache.s2graph.rest.play.Global.s2parser
  private val walLogHandler: ExceptionHandler = org.apache.s2graph.rest.play.Global.wallLogHandler

  private def enqueue(topic: String, elem: GraphElement, tsv: String) = {
    val kafkaMessage = ExceptionHandler.toKafkaMessage(topic, elem, Option(tsv))
    walLogHandler.enqueue(kafkaMessage)
  }

  private def publish(graphElem: GraphElement, tsv: String) = {
    val kafkaTopic = toKafkaTopic(graphElem.isAsync)

    graphElem match {
      case v: Vertex => enqueue(kafkaTopic, graphElem, tsv)
      case e: Edge =>
        e.label.extraOptions.get("walLog") match {
          case None => enqueue(kafkaTopic, e, tsv)
          case Some(walLogOpt) =>
            (walLogOpt \ "method").as[JsValue] match {
              case JsString("drop") => // pass
              case JsString("sample") =>
                val rate = (walLogOpt \ "rate").as[Double]
                if (scala.util.Random.nextDouble() < rate) enqueue(kafkaTopic, e, tsv)
              case _ => enqueue(kafkaTopic, e, tsv)
            }
        }
    }
  }

  private def tryMutate(elementsWithTsv: Seq[(GraphElement, String)], withWait: Boolean): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)
    else {
      try {
        elementsWithTsv.foreach { case (graphElem, tsv) =>
          publish(graphElem, tsv)
        }

        val elementsToStore = for {
          (e, _tsv) <- elementsWithTsv if !skipElement(e.isAsync)
        } yield e

        if (elementsToStore.isEmpty) Future.successful(jsonResponse(JsArray()))
        else {
          if (withWait) {
            val rets = s2.mutateElements(elementsToStore, withWait)
            rets.map(Json.toJson(_)).map(jsonResponse(_))
          } else {
            val rets = elementsToStore.map { element => QueueActor.router ! element; true }
            Future.successful(jsonResponse(Json.toJson(rets)))
          }
        }
      } catch {
        case e: GraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
        case e: Throwable =>
          logger.error(s"tryMutate: ${e.getMessage}", e)
          Future.successful(InternalServerError(s"${e.getStackTrace}"))
      }
    }
  }

  def mutateJsonFormat(jsValue: JsValue, operation: String, withWait: Boolean = false): Future[Result] = {
    logger.debug(s"$jsValue")
    val edgesWithTsv = requestParser.parseJsonFormat(jsValue, operation)
    tryMutate(edgesWithTsv, withWait)
  }

  def mutateBulkFormat(str: String, withWait: Boolean = false): Future[Result] = {
    logger.debug(s"$str")
    val elementsWithTsv = requestParser.parseBulkFormat(str)
    tryMutate(elementsWithTsv, withWait)
  }

  def mutateBulk() = withHeaderAsync(parse.text) { request =>
    mutateBulkFormat(request.body, withWait = false)
  }

  def mutateBulkWithWait() = withHeaderAsync(parse.text) { request =>
    mutateBulkFormat(request.body, withWait = true)
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "insert")
  }

  def insertsWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "insert", withWait = true)
  }

  def insertsBulk() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "insertBulk")
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "delete")
  }

  def deletesWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "delete", withWait = true)
  }

  def updates() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "update")
  }

  def updatesWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "update", withWait = true)
  }

  def increments() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "increment")
  }

  def incrementsWithWait() = withHeaderAsync(jsonParser) { request =>
    mutateJsonFormat(request.body, "increment", withWait = true)
  }

  def incrementCounts() = withHeaderAsync(jsonParser) { request =>
    val jsValue = request.body
    val edgesWithTsv = requestParser.parseJsonFormat(jsValue, "incrementCount")

    val edges = for {
      (e, _tsv) <- edgesWithTsv if !skipElement(e.isAsync)
    } yield e

    if (edges.isEmpty) Future.successful(jsonResponse(JsArray()))
    else {
      s2.incrementCounts(edges, withWait = true).map { results =>
        val json = results.map { case (isSuccess, resultCount) =>
          Json.obj("success" -> isSuccess, "result" -> resultCount)
        }
        jsonResponse(Json.toJson(json))
      }
    }
  }

  def deleteAll() = withHeaderAsync(jsonParser) { request =>
//    deleteAllInner(request.body, withWait = false)
    deleteAllInner(request.body, withWait = true)
  }

  def deleteAllInner(jsValue: JsValue, withWait: Boolean) = {

    /* logging for delete all request */
    def enqueueLogMessage(ids: Seq[JsValue], labels: Seq[Label], ts: Long, direction: String, topicOpt: Option[String]) = {
      val kafkaMessages = for {
        id <- ids
        label <- labels
      } yield {
        val tsv = Seq(ts, "deleteAll", "e", requestParser.jsToStr(id), requestParser.jsToStr(id), label.label, "{}", direction).mkString("\t")
        val topic = topicOpt.getOrElse { toKafkaTopic(label.isAsync) }

        ExceptionHandler.toKafkaMessage(topic, tsv)
      }

      kafkaMessages.foreach(walLogHandler.enqueue)
    }

    def deleteEach(labels: Seq[Label], direction: String, ids: Seq[JsValue], ts: Long, vertices: Seq[Vertex]) = {
      enqueueLogMessage(ids, labels, ts, direction, None)
      val future = s2.deleteAllAdjacentEdges(vertices.toList, labels, GraphUtil.directions(direction), ts)
      if (withWait) {
        future
      } else {
        Future.successful(true)
      }
    }

    val deleteFutures = jsValue.as[Seq[JsValue]].map { json =>
      val (labels, direction, ids, ts, vertices) = requestParser.toDeleteParam(json)
      if (labels.isEmpty || ids.isEmpty) Future.successful(true)
      else deleteEach(labels, direction, ids, ts, vertices)
    }

    val deleteResults = Future.sequence(deleteFutures)
    deleteResults.map { rst =>
      logger.debug(s"deleteAllInner: $rst")
      Ok(s"deleted... ${rst.toString()}")
    }
  }
}
