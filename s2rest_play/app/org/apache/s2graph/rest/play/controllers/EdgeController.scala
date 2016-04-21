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

object EdgeController extends Controller {

  import ApplicationController._
  import ExceptionHandler._
  import play.api.libs.concurrent.Execution.Implicits._

  private val s2: Graph = org.apache.s2graph.rest.play.Global.s2graph
  private val requestParser: RequestParser = org.apache.s2graph.rest.play.Global.s2parser
  private val walLogHandler: ExceptionHandler = org.apache.s2graph.rest.play.Global.wallLogHandler

  private def jsToStr(js: JsValue): String = js match {
    case JsString(s) => s
    case obj => obj.toString()
  }
  private def jsToStr(js: JsLookupResult): String = js.toOption.map(jsToStr).getOrElse("undefined")

  def toTsv(jsValue: JsValue, op: String): String = {
    val ts = jsToStr(jsValue \ "timestamp")
    val from = jsToStr(jsValue \ "from")
    val to = jsToStr(jsValue \ "to")
    val label = jsToStr(jsValue \ "label")
    val props = (jsValue \ "props").asOpt[JsObject].getOrElse(Json.obj())

    (jsValue \ "direction").asOpt[String] match {
      case None => Seq(ts, op, "e", from, to, label, props).mkString("\t")
      case Some(dir) => Seq(ts, op, "e", from, to, label, props, dir).mkString("\t")
    }
  }

  def tryMutates(jsValue: JsValue, operation: String, withWait: Boolean = false): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)

    else {
      try {
        logger.debug(s"$jsValue")
        val (edges, jsOrgs) = requestParser.toEdgesWithOrg(jsValue, operation)

        for ((edge, orgJs) <- edges.zip(jsOrgs)) {
          val kafkaTopic = toKafkaTopic(edge.isAsync)
          val kafkaMessage = ExceptionHandler.toKafkaMessage(kafkaTopic, edge, Option(toTsv(orgJs, operation)))
          walLogHandler.enqueue(kafkaMessage)
        }

        val edgesToStore = edges.filterNot(e => e.isAsync)

        if (withWait) {
          val rets = s2.mutateEdges(edgesToStore, withWait = true)
          rets.map(Json.toJson(_)).map(jsonResponse(_))
        } else {
          val rets = edgesToStore.map { edge => QueueActor.router ! edge; true }
          Future.successful(jsonResponse(Json.toJson(rets)))
        }
      } catch {
        case e: GraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
        case e: Exception =>
          logger.error(s"mutateAndPublish: $e", e)
          Future.successful(InternalServerError(s"${e.getStackTrace}"))
      }
    }
  }

  def mutateAndPublish(str: String, withWait: Boolean = false): Future[Result] = {
    if (!Config.IS_WRITE_SERVER) Future.successful(Unauthorized)

    logger.debug(s"$str")
    val edgeStrs = str.split("\\n")

    var vertexCnt = 0L
    var edgeCnt = 0L
    try {
      val elements =
        for (edgeStr <- edgeStrs; str <- GraphUtil.parseString(edgeStr); element <- Graph.toGraphElement(str)) yield {
          element match {
            case v: Vertex => vertexCnt += 1
            case e: Edge => edgeCnt += 1
          }
          val kafkaTopic = toKafkaTopic(element.isAsync)
          walLogHandler.enqueue(toKafkaMessage(kafkaTopic, element, Some(str)))
          element
        }

      //FIXME:
      val elementsToStore = elements.filterNot(e => e.isAsync)
      if (withWait) {
        val rets = s2.mutateElements(elementsToStore, withWait)
        rets.map(Json.toJson(_)).map(jsonResponse(_))
      } else {
        val rets = elementsToStore.map { element => QueueActor.router ! element; true }
        Future.successful(jsonResponse(Json.toJson(rets)))
      }
    } catch {
      case e: GraphExceptions.JsonParseException => Future.successful(BadRequest(s"$e"))
      case e: Throwable =>
        logger.error(s"mutateAndPublish: $e", e)
        Future.successful(InternalServerError(s"${e.getStackTrace}"))
    }
  }

  def mutateBulk() = withHeaderAsync(parse.text) { request =>
    mutateAndPublish(request.body, withWait = false)
  }

  def mutateBulkWithWait() = withHeaderAsync(parse.text) { request =>
    mutateAndPublish(request.body, withWait = true)
  }

  def inserts() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert")
  }

  def insertsWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insert", withWait = true)
  }

  def insertsBulk() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "insertBulk")
  }

  def deletes() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete")
  }

  def deletesWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "delete", withWait = true)
  }

  def updates() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "update")
  }

  def updatesWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "update", withWait = true)
  }

  def increments() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "increment")
  }

  def incrementsWithWait() = withHeaderAsync(jsonParser) { request =>
    tryMutates(request.body, "increment", withWait = true)
  }

  def incrementCounts() = withHeaderAsync(jsonParser) { request =>
    val jsValue = request.body
    val edges = requestParser.toEdges(jsValue, "incrementCount")

    s2.incrementCounts(edges, withWait = true).map { results =>
      val json = results.map { case (isSuccess, resultCount) =>
        Json.obj("success" -> isSuccess, "result" -> resultCount)
      }

      jsonResponse(Json.toJson(json))
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
        val tsv = Seq(ts, "deleteAll", "e", jsToStr(id), jsToStr(id), label.label, "{}", direction).mkString("\t")
        val topic = topicOpt.getOrElse {
          if (label.isAsync) Config.KAFKA_LOG_TOPIC_ASYNC else Config.KAFKA_LOG_TOPIC
        }

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
